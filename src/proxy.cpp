// libelliptics-proxy - smart proxy for Elliptics file storage
// Copyright (C) 2012 Anton Kortunov <toshik@yandex-team.ru>

// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.

#include <sstream>
#include <iostream>

#include <fcntl.h>
#include <errno.h>
#include <netdb.h>

#include <sys/socket.h>

#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/tokenizer.hpp>
#include <boost/scoped_array.hpp>

#include <elliptics/proxy.hpp>
#include <msgpack.hpp>

#include "boost_threaded.hpp"
#include "curl_wrapper.hpp"
#include "logger.hpp"

namespace {
size_t uploads_need(size_t success_copies_num, size_t replication_count) {
	switch (success_copies_num) {
	case elliptics::SUCCESS_COPIES_TYPE__ANY:
		return 1;
	case elliptics::SUCCESS_COPIES_TYPE__QUORUM:
		return (replication_count >> 1) + 1;
	case elliptics::SUCCESS_COPIES_TYPE__ALL:
		return replication_count;
	default:
		return replication_count;
	}
}

bool upload_is_good(size_t success_copies_num, size_t replication_count, size_t size) {
	switch (success_copies_num) {
	case elliptics::SUCCESS_COPIES_TYPE__ANY:
		return size >= 1;
	case elliptics::SUCCESS_COPIES_TYPE__QUORUM:
		return size >= ((replication_count >> 1) + 1);
	case elliptics::SUCCESS_COPIES_TYPE__ALL:
		return size == replication_count;
	default:
		return size >= success_copies_num;
	}
}

class write_helper_t {
public:
	typedef std::vector<elliptics::LookupResult> lookup_results_t;
	typedef std::vector<int> groups_t;

	write_helper_t(int success_copies_num, int replication_count, const groups_t desired_groups)
		: m_success_copies_num(success_copies_num)
		, m_replication_count(replication_count)
		, m_desired_groups(desired_groups)
	{
	}

	void update_lookup(const lookup_results_t &tmp, bool update_ret = true) {
		groups_t groups;
		const size_t size = tmp.size();
		groups.reserve(size);

		if (update_ret) {
			m_ret.reserve(m_ret.size() + size);
			m_ret.insert(m_ret.end(), tmp.begin(), tmp.end());
		}

		for (auto it = tmp.begin(), end = tmp.end(); it != end; ++it) {
			groups.push_back(it->group);
		}

		m_upload_groups.swap(groups);
	}

	void fix_size(size_t size) {
		std::string str_size = boost::lexical_cast<std::string>(size);
		for (auto it = m_ret.begin(), end = m_ret.end(); it != end; ++it) {
			std::string &path = it->path;
			path.replace(path.begin() + path.rfind(':') + 1, path.end(), str_size);
		}
	}

	const groups_t &get_upload_groups() const {
		return m_upload_groups;
	}

	bool upload_is_good() const {
		return ::upload_is_good(m_success_copies_num, m_replication_count, m_upload_groups.size());
	}

	bool has_incomplete_groups() const {
		return m_desired_groups.size() != m_upload_groups.size();
	}

	groups_t get_incomplete_groups() {
		groups_t incomplete_groups;
		incomplete_groups.reserve(m_desired_groups.size() - m_upload_groups.size());
		std::sort(m_desired_groups.begin(), m_desired_groups.end());
		std::sort(m_upload_groups.begin(), m_upload_groups.end());
		std::set_difference(m_desired_groups.begin(), m_desired_groups.end(),
							 m_upload_groups.begin(), m_upload_groups.end(),
							 std::back_inserter(incomplete_groups));
		return incomplete_groups;
	}

	const lookup_results_t &get_result() const {
		return m_ret;
	}

private:

	int m_success_copies_num;
	int m_replication_count;
	//
	lookup_results_t m_ret;
	groups_t m_desired_groups;
	groups_t m_upload_groups;

};

struct dnet_id_less {
	bool operator () (const struct dnet_id &ob1, const struct dnet_id &ob2) {
		int res = memcmp(ob1.id, ob2.id, DNET_ID_SIZE);
		if (res == 0)
			res = ob1.type - ob2.type;
		return (res < 0);
	}
};
} // namespace

using namespace ioremap::elliptics;

#ifdef HAVE_METABASE
namespace msgpack {
    inline elliptics::GroupInfoResponse& operator >> (object o, elliptics::GroupInfoResponse& v) {
        if (o.type != type::MAP) { 
            throw type_error();
        }

        msgpack::object_kv* p = o.via.map.ptr;
        msgpack::object_kv* const pend = o.via.map.ptr + o.via.map.size;

        for (; p < pend; ++p) {
            std::string key;

            p->key.convert(&key);

//            if (!key.compare("nodes")) {
//                p->val.convert(&(v.nodes));
//            }
            if (!key.compare("couples")) {
                p->val.convert(&(v.couples));
            }
            else if (!key.compare("status")) {
		std::string status;
                p->val.convert(&status);
		if (!status.compare("bad")) {
			v.status = elliptics::GROUP_INFO_STATUS_BAD;
		} else if (!status.compare("coupled")) {
			v.status = elliptics::GROUP_INFO_STATUS_COUPLED;
		}
            }
        }

        return v;
    }

    inline elliptics::MetabaseGroupWeightsResponse& operator >> (
            object o,
            elliptics::MetabaseGroupWeightsResponse& v) {
        if (o.type != type::MAP) {
            throw type_error();
        }

        msgpack::object_kv* p = o.via.map.ptr;
        msgpack::object_kv* const pend = o.via.map.ptr + o.via.map.size;

        for (; p < pend; ++p) {
            elliptics::MetabaseGroupWeightsResponse::SizedGroups sized_groups;
            p->key.convert(&sized_groups.size);
            p->val.convert(&sized_groups.weighted_groups);
            v.info.push_back(sized_groups);
        }

        return v;
    }
}
#endif /* HAVE_METABASE */

namespace elliptics {

enum dnet_common_embed_types {
	DNET_PROXY_EMBED_DATA	    = 1,
	DNET_PROXY_EMBED_TIMESTAMP
};

struct dnet_common_embed {
	uint64_t		size;
	uint32_t		type;
	uint32_t		flags;
	uint8_t			data[0];
};

static inline void
dnet_common_convert_embedded(struct dnet_common_embed *e) {
	e->size = dnet_bswap64(e->size);
	e->type = dnet_bswap32(e->type);
	e->flags = dnet_bswap32(e->flags);
}

EllipticsProxy::EllipticsProxy(const EllipticsProxy::config &c) :
				groups_(c.groups),
				base_port_(c.base_port),
				directory_bit_num_(c.directory_bit_num),
				success_copies_num_(c.success_copies_num),
				state_num_(c.state_num),
				replication_count_(c.replication_count),
				chunk_size_(c.chunk_size),
				eblob_style_path_(c.eblob_style_path)
#ifdef HAVE_METABASE
				,metabase_usage_(PROXY_META_NONE)
				,metabase_write_addr_(c.metabase_write_addr)
				,metabase_read_addr_(c.metabase_read_addr)
#endif /* HAVE_METABASE */
{
	if (!c.remotes.size()) {
		throw std::runtime_error("Remotes can't be empty");
	}

	struct dnet_config dnet_conf;
	memset(&dnet_conf, 0, sizeof (dnet_conf));

	dnet_conf.wait_timeout = c.wait_timeout;
	dnet_conf.check_timeout = c.check_timeout;
	dnet_conf.flags = c.flags;

	if (c.ns.size()) {
		dnet_conf.ns = const_cast<char *>(c.ns.data());
		dnet_conf.nsize = c.ns.size();
	}

	elliptics_log_.reset(new ioremap::elliptics::file_logger(c.log_path.c_str(), c.log_mask));
	elliptics_node_.reset(new ioremap::elliptics::node(*elliptics_log_, dnet_conf));

	for (std::vector<EllipticsProxy::remote>::const_iterator it = c.remotes.begin(); it != c.remotes.end(); ++it) {
		try {
			elliptics_node_->add_remote(it->host.c_str(), it->port, it->family);
		} catch(const std::exception &e) {
			std::stringstream msg;
			msg << "Can't connect to remote node " << it->host << ":" << it->port << ":" << it->family << " : " << e.what() << std::endl;
			elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
		}
	}
#ifdef HAVE_METABASE
	std::shared_ptr<cocaine::framework::logger_t> mastermind_logger = c.mastermind_logger;
	if (!mastermind_logger) {
		mastermind_logger = std::make_shared<cocaine_logger_t>(*elliptics_log_);
	}

	mastermind_ = std::make_shared<elliptics::mastermind_t>(
		c.mastermind_host
		, c.mastermind_port
		, mastermind_logger
		, c.group_weights_refresh_period
		);
#endif /* HAVE_METABASE */
}

std::vector<LookupResult>
EllipticsProxy::parse_lookup(Key &key, std::string &l)
{
	std::vector<LookupResult> ret;
	size_t size = l.size();
	const size_t min_size = sizeof(struct dnet_addr) +
				sizeof(struct dnet_cmd) +
				sizeof(struct dnet_addr_attr) +
				sizeof(struct dnet_file_info);
	const char *data = (const char *)l.data();

	while (size > min_size) {
		LookupResult result;

		struct dnet_addr *addr = (struct dnet_addr *)data;
		struct dnet_cmd *cmd = (struct dnet_cmd *)(addr + 1);
		struct dnet_addr_attr *a = (struct dnet_addr_attr *)(cmd + 1);
		struct dnet_file_info *info = (struct dnet_file_info *)(a + 1);
		dnet_convert_file_info(info);
		result.id = cmd->id;

		char hbuf[NI_MAXHOST];
		memset(hbuf, 0, NI_MAXHOST);

		if (getnameinfo((const sockaddr*)&a->addr, a->addr.addr_len, hbuf, sizeof (hbuf), NULL, 0, 0) != 0) {
			throw std::runtime_error("can not make dns lookup");
		}
		result.hostname.assign(hbuf);

		result.port = dnet_server_convert_port((struct sockaddr *)a->addr.addr, a->addr.addr_len);
		result.group = cmd->id.group_id;

		if (eblob_style_path_) {
			result.path = std::string((char *)(info + 1));
			result.path = result.path.substr(result.path.find_last_of("/\\") + 1);
			result.path = "/" + boost::lexical_cast<std::string>(result.port - base_port_) + '/' 
				+ result.path + ":" + boost::lexical_cast<std::string>(info->offset)
				+ ":" +  boost::lexical_cast<std::string>(info->size);
		} else {
			//struct dnet_id id;
			//elliptics_node_->transform(key.filename(), id);
			//result.path = "/" + boost::lexical_cast<std::string>(port - base_port_) + '/' + hex_dir + '/' + id;
		}

		ret.push_back(result);

		data += min_size + info->flen;
		size -= min_size + info->flen;
	}

	return ret;
}

LookupResult EllipticsProxy::lookup_impl(Key &key, std::vector<int> &groups)
{
	session elliptics_session(*elliptics_node_);
	std::vector<int> lgroups;
	LookupResult result;

	lgroups = getGroups(key, groups);
	try {
		elliptics_session.set_groups(lgroups);
		std::string l;
		if (key.byId()) {
			struct dnet_id id = key.id().dnet_id();
			l = elliptics_session.lookup(id);
		} else {
			l = elliptics_session.lookup(key.filename());
		}

		result = parse_lookup(key, l)[0];

	}
	catch (const std::exception &e) {
		std::stringstream msg;
		msg << "can not get download info for key " << key.str() << " " << e.what();
		elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
		throw;
	}
	catch (...) {
		std::stringstream msg;
		msg << "can not get download info for key " << key.str();
		elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
		throw;
	}

	return result;
}

namespace {
bool compare_mtimes(const std::pair<int, dnet_time> &p1, const std::pair<int, dnet_time> &p2)
{
	if (p1.second.tsec == p2.second.tsec) {
		return p1.second.tnsec > p2.second.tnsec;
	}

	return p1.second.tsec > p2.second.tsec;
}

std::vector<std::pair<int, dnet_time> >
parse_latest(const std::string &data)
{
	const char *ptr = data.data();
	size_t size = data.size();

	std::vector<std::pair<int, dnet_time> > mtimes;

	while (size > 0) {
		if (size < (sizeof(dnet_addr) + sizeof(struct dnet_cmd)))
			throw std::logic_error("Data size is too low");

		ptr += sizeof(dnet_addr);
		size -= sizeof(dnet_addr);

		struct dnet_cmd *cmd = (struct dnet_cmd *)ptr;
		if (cmd->size == 0) {
			ptr += sizeof(dnet_cmd);
			size -= sizeof(dnet_cmd);
			continue;
		}

		if (cmd->size < sizeof(struct dnet_addr_attr) + sizeof(struct dnet_file_info))
			throw std::logic_error("CMD size is too low");

		struct dnet_addr_attr *a;
		struct dnet_file_info *fi;

		a = (struct dnet_addr_attr *)(cmd + 1);
		fi = (struct dnet_file_info *)(a + 1);

		dnet_time mtime = {fi->mtime.tsec, fi->mtime.tnsec};
		int group_id = cmd->id.group_id;
		mtimes.push_back(std::make_pair(group_id, mtime));

		ptr += sizeof(struct dnet_cmd) + cmd->size;
		size -= sizeof(struct dnet_cmd) + cmd->size;
	}

	std::sort(mtimes.begin(), mtimes.end(), compare_mtimes);

	if (mtimes.size() > 1) {
		auto it = std::find_if(mtimes.begin() + 1, mtimes.end(), std::bind(compare_mtimes, *mtimes.begin(), std::placeholders::_1));
		mtimes.erase(it, mtimes.end());
	}

	
	return mtimes;
}
}

std::vector<std::pair<int, dnet_time> >
EllipticsProxy::prepare_latest_impl(Key &key, std::vector<int> &groups)
{
	session elliptics_session(*elliptics_node_);
	std::vector<int> lgroups = getGroups(key, groups);
	std::string res;

	elliptics_session.set_cflags(DNET_ATTR_META_TIMES);

	try {
		key.transform(elliptics_session);

		callback_any c;
		dnet_id raw = key.id().dnet_id();
		int count = 0;
		std::vector<int> group(1, 0);

		for (auto g = lgroups.begin(); g != lgroups.end(); ++g) {
			group[0] = *g;
			elliptics_session.set_groups(group);
			raw.group_id = *g;

			try {
				elliptics_session.lookup(raw, c);
			} catch (...) {}
			count++;
		}

		res = c.wait(count);
	} catch (std::exception &e) {
		std::cerr << "Got exception " << e.what() << std::endl;
	}

	return parse_latest(res);
}

ReadResult
EllipticsProxy::read_impl(Key &key, uint64_t offset, uint64_t size,
				uint64_t cflags, uint64_t ioflags, std::vector<int> &groups,
				bool latest, bool embeded)
{
	session elliptics_session(*elliptics_node_);
	std::vector<int> lgroups;
	lgroups = getGroups(key, groups);

	elliptics_session.set_cflags(cflags);
	elliptics_session.set_ioflags(ioflags);

	std::string result;
	ReadResult ret;

	try {
		elliptics_session.set_groups(lgroups);

		if (latest)
			result = elliptics_session.read_latest(key, offset, size);
		else
			result = elliptics_session.read_data_wait(key, offset, size);

		if (embeded) {
			size_t size = result.size();
			size_t offset = 0;

			while (size) {
				struct dnet_common_embed *e = (struct dnet_common_embed *)(result.data() + offset);

				dnet_common_convert_embedded(e);

				if (size < sizeof(struct dnet_common_embed)) {
					std::ostringstream str;
					str << key.str() << ": offset: " << offset << ", size: " << size << ": invalid size";
					throw std::runtime_error(str.str());
				}

				offset += sizeof(struct dnet_common_embed);
				size -= sizeof(struct dnet_common_embed);

				if (size < e->size + sizeof (struct dnet_common_embed)) {
					break;
				}

				if (e->type == DNET_PROXY_EMBED_DATA) {
					size = e->size;
					break;
				}

				offset += e->size;
				size -= e->size;
			}
			ret.data = result.substr(offset, std::string::npos);
		} else {
			ret.data = result;
		}

	}
	catch (const std::exception &e) {
		std::stringstream msg;
		msg << "can not get data for key " << key.str() << " " << e.what() << std::endl;
		elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
		throw;
	}
	catch (...) {
		std::stringstream msg;
		msg << "can not get data for key " << key.str() << std::endl;
		elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
		throw;
	}

	return ret;
}

std::vector<LookupResult> EllipticsProxy::write_impl(Key &key, std::string &data, uint64_t offset, uint64_t size,
					uint64_t cflags, uint64_t ioflags, std::vector<int> &groups,
					unsigned int replication_count, std::vector<boost::shared_ptr<embed> > embeds)
{
	session elliptics_session(*elliptics_node_);
	bool use_metabase = false;

	elliptics_session.set_cflags(cflags);
	elliptics_session.set_ioflags(ioflags);

	if (elliptics_session.state_num() < state_num_) {
		throw std::runtime_error("Too low number of existing states");
	}

	if (replication_count == 0) {
		replication_count = replication_count_;
	}

	std::vector<int> lgroups = getGroups(key, groups);
#ifdef HAVE_METABASE
	if (metabase_usage_ >= PROXY_META_OPTIONAL) {
		try {
			if (groups.size() != replication_count || metabase_usage_ == PROXY_META_MANDATORY) {
				std::vector<int> mgroups = get_metabalancer_groups_impl(replication_count, size, key);
				lgroups = mgroups;
			}
			use_metabase = 1;
		} catch (std::exception &e) {
			elliptics_log_->log(DNET_LOG_ERROR, e.what());
			if (metabase_usage_ >= PROXY_META_NORMAL) {
				throw std::runtime_error("Metabase does not respond");
			}
		}
	}
#endif /* HAVE_METABASE */
	if (replication_count != 0 && (size_t)replication_count < lgroups.size())
		lgroups.erase(lgroups.begin() + replication_count, lgroups.end());

	write_helper_t helper(success_copies_num_, replication_count, lgroups);

	try {
		elliptics_session.set_groups(lgroups);

		bool chunked = false;

		std::string content;

		for (std::vector<boost::shared_ptr<embed> >::const_iterator it = embeds.begin(); it != embeds.end(); it++) {
			content.append((*it)->pack());
		}
		content.append(data);

		if (chunk_size_ && content.size() > static_cast<size_t>(chunk_size_) && !key.byId()
				&& !(ioflags & (DNET_IO_FLAGS_PREPARE | DNET_IO_FLAGS_COMMIT | DNET_IO_FLAGS_PLAIN_WRITE))) {
			chunked = true;
		}

		//ioremap::elliptics::write_result lookup;
		std::string lookup;

		struct dnet_id id;
		memset(&id, 0, sizeof(id));

		if (key.byId()) {
			id = key.id().dnet_id();
		} else {
			elliptics_session.transform(key.filename(), id);
			id.type = key.column();
		}

		try {
			if (key.byId()) {
				lookup = elliptics_session.write_data_wait(id, content, offset);
			} else {
				if (ioflags & DNET_IO_FLAGS_PREPARE) {
					lookup = elliptics_session.write_prepare(key, content, offset, size);
				} else if (ioflags & DNET_IO_FLAGS_COMMIT) {
					lookup = elliptics_session.write_commit(key, content, offset, size);
				} else if (ioflags & DNET_IO_FLAGS_PLAIN_WRITE) {
					lookup = elliptics_session.write_plain(key, content, offset);
				} else {
					if (chunked) {
						std::string write_content;
						bool first_iter = true;
						size_t size = content.size();

						content.substr(offset, chunk_size_).swap(write_content);
						lookup = elliptics_session.write_prepare(key, write_content, offset, content.size());
						helper.update_lookup(parse_lookup(key, lookup), false);

						if (helper.upload_is_good()) {
							do {
								elliptics_session.set_groups(helper.get_upload_groups());
								offset += chunk_size_;
								content.substr(offset, chunk_size_).swap(write_content);

								if (offset + chunk_size_ >= content.length())
									lookup = elliptics_session.write_commit(key, write_content, offset, 0);
								else
									lookup = elliptics_session.write_plain(key, write_content, offset);
								helper.update_lookup(parse_lookup(key, lookup), first_iter);
								first_iter = false;
							} while (helper.upload_is_good() && (offset + chunk_size_ < content.length()));
						}

						helper.fix_size(size);

					} else {
						lookup = elliptics_session.write_data_wait(key, content, offset);
					}
				}
			}

			if (!chunked)
				helper.update_lookup(parse_lookup(key, lookup));

			if (!helper.upload_is_good()) {
				elliptics_session.set_groups(lgroups);
				elliptics_session.remove(key.filename());
				throw std::runtime_error("Not enough copies was written, or problems with chunked upload");
			}

			if (chunked && helper.has_incomplete_groups()) {
				elliptics_session.set_groups(helper.get_incomplete_groups());
				elliptics_session.remove(key.filename());
			}

		}
		catch (const std::exception &e) {
			std::stringstream msg;
			msg << "Can't write data for key " << key.str() << " " << e.what();
			elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
			throw;
		}
		catch (...) {
			elliptics_log_->log(DNET_LOG_ERROR, "Can't write data for key: unknown");
			throw;
		}

		struct timespec ts;
		memset(&ts, 0, sizeof(ts));

		elliptics_session.set_cflags(0);
		elliptics_session.write_metadata(key, key.filename(), helper.get_upload_groups(), ts);
		elliptics_session.set_cflags(ioflags);
#ifdef HAVE_METABASE
		if (!metabase_write_addr_.empty() && !metabase_read_addr_.empty()) {
			uploadMetaInfo(lgroups, key);
		}
#endif /* HAVE_METABASE */
	}
	catch (const std::exception &e) {
		std::stringstream msg;
		msg << "Can't write data for key " << key.str() << " " << e.what();
		elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
		throw;
	}
	catch (...) {
		std::stringstream msg;
		msg << "Can't write data for key " << key.str();
		elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
		throw;
	}

	return helper.get_result();
}

std::vector<std::string> EllipticsProxy::range_get_impl(Key &from, Key &to, uint64_t cflags, uint64_t ioflags,
					uint64_t limit_start, uint64_t limit_num, const std::vector<int> &groups, Key &key)
{
	session elliptics_session(*elliptics_node_);
	std::vector<int> lgroups;
	lgroups = getGroups(key, groups);

	elliptics_session.set_cflags(cflags);
	elliptics_session.set_ioflags(ioflags);

	std::vector<std::string> ret;

	try {
		struct dnet_io_attr io;
		memset(&io, 0, sizeof(struct dnet_io_attr));

		if (from.byId()) {
			memcpy(io.id, from.id().dnet_id().id, sizeof(io.id));
		}

		if (to.byId()) {
			memcpy(io.parent, from.id().dnet_id().id, sizeof(io.parent));
		} else {
			memset(io.parent, 0xff, sizeof(io.parent));
		}

		io.start = limit_start;
		io.num = limit_num;
		io.flags = ioflags;
		io.type = from.column();


		for (size_t i = 0; i < lgroups.size(); ++i) {
			try {
				ret = elliptics_session.read_data_range(io, lgroups[i]);
				if (ret.size())
					break;
			} catch (...) {
				continue;
			}
		}

		if (ret.size() == 0) {
			std::ostringstream str;
			str << "READ_RANGE failed for key " << key.str() << " in " << groups.size() << " groups";
			throw std::runtime_error(str.str());
		}


	}
	catch (const std::exception &e) {
		std::stringstream msg;
		msg << "READ_RANGE failed for key " << key.str() << " from:" << from.str() << " to:" << to.str() << " " << e.what();
		elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
		throw;
	}
	catch (...) {
		std::stringstream msg;
		msg << "READ_RANGE failed for key " << key.str() << " from:" << from.str() << " to:" << to.str();
		elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
		throw;
	}

	return ret;
}

void EllipticsProxy::remove_impl(Key &key, std::vector<int> &groups)
{
	session elliptics_session(*elliptics_node_);
	std::vector<int> lgroups;

	lgroups = getGroups(key, groups);
	try {
		elliptics_session.set_groups(lgroups);
		std::string l;
		if (key.byId()) {
			struct dnet_id id = key.id().dnet_id();
			int error = -1;

			for (size_t i = 0; i < lgroups.size(); ++i) {
				id.group_id = lgroups[i];
				try {
					elliptics_session.remove(id);
					error = 0;
				} catch (const std::exception &e) {
					std::stringstream msg;
					msg << "Can't remove object " << key.str() << " in group " << groups[i] << ": " << e.what();
					elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
				}
			}

			if (error) {
				std::ostringstream str;
				str << dnet_dump_id(&id) << ": REMOVE failed";
				throw std::runtime_error(str.str());
			}
		} else {
			elliptics_session.remove(key.filename());
		}

	}
	catch (const std::exception &e) {
		std::stringstream msg;
		msg << "Can't remove object " << key.str() << " " << e.what();
		elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
		throw;
	}
	catch (...) {
		std::stringstream msg;
		msg << "Can't remove object " << key.str();
		elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
		throw;
	}
}

std::map<Key, ReadResult>
EllipticsProxy::bulk_read_impl(std::vector<Key> &keys, uint64_t cflags, std::vector<int> &groups)
{
	std::map<Key, ReadResult> ret;

        if (!keys.size())
                return ret;

	session elliptics_session(*elliptics_node_);
	std::vector<int> lgroups = getGroups(keys[0], groups);

	std::vector<std::string> result;
	std::map<ID, Key> keys_transformed;

	try {
		elliptics_session.set_groups(lgroups);

                std::vector<struct dnet_io_attr> ios;
                ios.reserve(keys.size());

                for (std::vector<Key>::iterator it = keys.begin(); it != keys.end(); it++) {
                        struct dnet_io_attr io;
                        memset(&io, 0, sizeof(io));

                        Key tmp(*it);
                        if (!tmp.byId()) {
                        
                                tmp.transform(elliptics_session);
                        }


                        memcpy(io.id, tmp.id().dnet_id().id, sizeof(io.id));
                        ios.push_back(io);
                        keys_transformed.insert(std::make_pair(tmp.id(), *it));
                }

		result = elliptics_session.bulk_read(ios);

                for (std::vector<std::string>::iterator it = result.begin();
                                                 it != result.end(); it++) {

                        if (it->size() < DNET_ID_SIZE + 8)
                                throw std::runtime_error("Too small record came from bulk_read");

                        struct dnet_id id;
                        uint64_t *size;

                        memset(&id, 0, sizeof(id));
                        memcpy(id.id, it->data(), DNET_ID_SIZE);
                        size = (uint64_t *)(it->data() + DNET_ID_SIZE);

                        if (*size != (it->size() - DNET_ID_SIZE - 8))
                                throw std::runtime_error("Too small record came from bulk_read");

                        ID ell_id(id);
	                ReadResult tmp;
                        tmp.data.assign(it->data() + DNET_ID_SIZE + 8, *size);

                        ret.insert(std::make_pair(keys_transformed[ell_id], tmp));
                }

	}
	catch (const std::exception &e) {
		std::stringstream msg;
		msg << "can not bulk get data " << e.what() << std::endl;
		elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
		throw;
	}
	catch (...) {
		std::stringstream msg;
		msg << "can not bulk get data" << std::endl;
		elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
		throw;
	}

	return ret;
}

std::vector<EllipticsProxy::remote> EllipticsProxy::lookup_addr_impl(Key &key, std::vector<int> &groups)
{
	session elliptics_session(*elliptics_node_);
	std::vector<int> lgroups = getGroups(key, groups);

    std::vector<EllipticsProxy::remote> addrs;

    for (std::vector<int>::const_iterator it = groups.begin();
            it != groups.end(); it++)
    {
        std::string ret;

        if (key.byId()) {
            struct dnet_id id = key.id().dnet_id();
	    ret = elliptics_session.lookup_address(id, *it);

        } else {
	    ret = elliptics_session.lookup_address(key.filename(), *it);
        }

        size_t pos = ret.find(':');
        EllipticsProxy::remote addr(ret.substr(0, pos), boost::lexical_cast<int>(ret.substr(pos+1, std::string::npos)));

        addrs.push_back(addr);
    }

    return addrs;
}

std::map<Key, std::vector<LookupResult> > EllipticsProxy::bulk_write_impl(std::vector<Key> &keys, std::vector<std::string> &data, uint64_t cflags,
																			   std::vector<int> &groups, int success_copies_num) {
	unsigned int replication_count = groups.size();
	std::map<Key, std::vector<LookupResult> > res;
	std::map<Key, std::vector<int> > res_groups;

	if (!keys.size())
		return res;

	session elliptics_session(*elliptics_node_);

	if (replication_count == 0)
		replication_count = replication_count_;

	std::vector<int> lgroups = getGroups(keys[0], groups);

	std::map<struct dnet_id, Key, dnet_id_less> keys_transformed;

	try {
		if (keys.size() != data.size())
			throw std::runtime_error("counts of keys and data are not equal");

		elliptics_session.set_groups(lgroups);

		std::vector<struct dnet_io_attr> ios;
		ios.reserve(keys.size());

		for (size_t index = 0; index != keys.size(); ++index) {
			struct dnet_io_attr io;
			memset(&io, 0, sizeof(io));

			Key tmp(keys [index]);
			if (!tmp.byId()) {
				tmp.transform(elliptics_session);
			}

			memcpy(io.id, tmp.id().dnet_id().id, sizeof(io.id));
			io.size = data[index].size();
			ios.push_back(io);
			keys_transformed.insert(std::make_pair(tmp.id().dnet_id(), keys [index]));
		}

		std::string lookup = elliptics_session.bulk_write(ios, data);
		std::vector<LookupResult> result = parse_lookup(keys.front(), lookup);

		//for (size_t i = 0; i != result.size(); ++i) {
		for (auto it = result.begin(), end = result.end(); it != end; ++it) {
			const LookupResult &lr = *it;
			Key key = keys_transformed [lr.id];
			res [key].push_back(lr);
			res_groups [key].push_back(lr.group);
		}

		unsigned int replication_need = uploads_need(success_copies_num != 0 ? success_copies_num : success_copies_num_,
													 replication_count);

		auto it = res_groups.begin();
		auto end = res_groups.end();
		for (; it != end; ++it) {
			if (it->second.size() < replication_need)
				break;
		}

		if (it != end) {
			for (auto it = res_groups.begin(), end = res_groups.end(); it != end; ++it) {
				elliptics_session.set_groups(it->second);
				elliptics_session.remove(it->first);
			}
			throw std::runtime_error("Not enough copies was written");
		}

	}
	catch (const std::exception &e) {
		std::stringstream msg;
		msg << "can not bulk write data " << e.what() << std::endl;
		elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
		throw;
	}
	catch (...) {
		std::stringstream msg;
		msg << "can not bulk write data" << std::endl;
		elliptics_log_->log(DNET_LOG_ERROR, msg.str().c_str());
		throw;
	}

	return res;
}

/*
void
EllipticsProxy::rangeDeleteHandler(fastcgi::Request *request) {
	std::string filename = request->hasArg("name") ? request->getArg("name") :
		request->getScriptName().substr(sizeof ("/range/") - 1, std::string::npos);

	std::vector<int> groups;
	if (!metabase_write_addr_.empty() && !metabase_read_addr_.empty()) {
		try {
			groups = getMetaInfo(filename);
		}
		catch (...) {
			groups = getGroups(request);
		}
	}
	else {
		groups = getGroups(request);
	}

	std::string extention = filename.substr(filename.rfind('.') + 1, std::string::npos);

	if (deny_list_.find(extention) != deny_list_.end() ||
		(deny_list_.find("*") != deny_list_.end() &&
		allow_list_.find(extention) == allow_list_.end())) {
		throw fastcgi::HttpException(403);
	}

	std::map<std::string, std::string>::iterator it = typemap_.find(extention);

	std::string content_type = "application/octet";

	try {
		unsigned int aflags = request->hasArg("aflags") ? boost::lexical_cast<unsigned int>(request->getArg("aflags")) : 0;
		unsigned int ioflags = request->hasArg("ioflags") ? boost::lexical_cast<unsigned int>(request->getArg("ioflags")) : 0;
		int column = request->hasArg("column") ? boost::lexical_cast<int>(request->getArg("column")) : 0;

		struct dnet_io_attr io;
		memset(&io, 0, sizeof(struct dnet_io_attr));

		struct dnet_id tmp;

		if (request->hasArg("from")) {
			dnet_parse_numeric_id(request->getArg("from"), tmp);
			memcpy(io.id, tmp.id, sizeof(io.id));
		}

		if (request->hasArg("to")) {
			dnet_parse_numeric_id(request->getArg("to"), tmp);
			memcpy(io.parent, tmp.id, sizeof(io.parent));
		} else {
			memset(io.parent, 0xff, sizeof(io.parent));
		}

		io.flags = ioflags;
		io.type = column;

		std::vector<struct dnet_io_attr> ret;

		for (size_t i = 0; i < groups.size(); ++i) {
			try {
				ret = elliptics_node_->remove_data_range(io, groups[i], aflags);
				break;
			} catch (...) {
				continue;
			}
		}

		if (ret.size() == 0) {
			std::ostringstream str;
			str << filename.c_str() << ": REMOVE_RANGE failed in " << groups.size() << " groups";
			throw std::runtime_error(str.str());
		}

		std::string result;
		int removed = 0;

		for (size_t i = 0; i < ret.size(); ++i) {
			removed += ret[i].num;
		}

		result = boost::lexical_cast<std::string>(removed);

		request->setStatus(200);
		request->setContentType(content_type);
		request->setHeader("Content-Length", boost::lexical_cast<std::string>(result.length()));
		request->write(result.data(), result.size());
	}
	catch (const std::exception &e) {
		log()->error("%s: REMOVE_RANGE failed: %s", filename.c_str(), e.what());
		request->setStatus(404);
	}
	catch (...) {
		log()->error("%s: REMOVE_RANGE failed", filename.c_str());
		request->setStatus(404);
	}
}

void
EllipticsProxy::statLogHandler(fastcgi::Request *request) {
	std::string result = "<?xml version=\"1.0\" encoding=\"utf-8\"?>";

	result += "<data>\n";

	std::string ret = elliptics_node_->stat_log();

	float la[3];
	const void *data = ret.data();
	int size = ret.size();
	char id_str[DNET_ID_SIZE * 2 + 1];
	char addr_str[128];

	while (size) {
		struct dnet_addr *addr = (struct dnet_addr *)data;
		struct dnet_cmd *cmd = (struct dnet_cmd *)(addr + 1);
		if (cmd->size != sizeof (struct dnet_attr) + sizeof (struct dnet_stat)) {
			size -= cmd->size + sizeof (struct dnet_addr) + sizeof (struct dnet_cmd);
			data = (char *)data + cmd->size + sizeof (struct dnet_addr) + sizeof (struct dnet_cmd);
			continue;
		}
		struct dnet_attr *attr = (struct dnet_attr *)(cmd + 1);
		struct dnet_stat *st = (struct dnet_stat *)(attr + 1);

		dnet_convert_stat(st);

		la[0] = (float)st->la[0] / 100.0;
		la[1] = (float)st->la[1] / 100.0;
		la[2] = (float)st->la[2] / 100.0;

		char buf[512];

		snprintf(buf, sizeof (buf), "<stat addr=\"%s\" id=\"%s\"><la>%.2f %.2f %.2f</la>"
			"<memtotal>%llu KB</memtotal><memfree>%llu KB</memfree><memcached>%llu KB</memcached>"
			"<storage_size>%llu MB</storage_size><available_size>%llu MB</available_size>"
			"<files>%llu</files><fsid>0x%llx</fsid></stat>",
			dnet_server_convert_dnet_addr_raw(addr, addr_str, sizeof(addr_str)),
			dnet_dump_id_len_raw(cmd->id.id, DNET_ID_SIZE, id_str),
			la[0], la[1], la[2],
			(unsigned long long)st->vm_total,
			(unsigned long long)st->vm_free,
			(unsigned long long)st->vm_cached,
			(unsigned long long)(st->frsize * st->blocks / 1024 / 1024),
			(unsigned long long)(st->bavail * st->bsize / 1024 / 1024),
			(unsigned long long)st->files, (unsigned long long)st->fsid);

		result += buf;

		int sz = sizeof(*addr) + sizeof(*cmd) + sizeof(*attr) + attr->size;
		size -= sz;
		data = (char *)data + sz;
	}

	result += "</data>";

	request->setStatus(200);
	request->write(result.c_str(), result.size());
}

void
EllipticsProxy::dnet_parse_numeric_id(const std::string &value, struct dnet_id &id)
{
	unsigned char ch[5];
	unsigned int i, len = value.size();
	const char *ptr = value.data();

	memset(id.id, 0, DNET_ID_SIZE);

	if (len/2 > DNET_ID_SIZE)
		len = DNET_ID_SIZE * 2;

	ch[0] = '0';
	ch[1] = 'x';
	ch[4] = '\0';
	for (i=0; i<len / 2; i++) {
		ch[2] = ptr[2*i + 0];
		ch[3] = ptr[2*i + 1];

		id.id[i] = (unsigned char)strtol((const char *)ch, NULL, 16);
	}

	if (len & 1) {
		ch[2] = ptr[2*i + 0];
		ch[3] = '0';

		id.id[i] = (unsigned char)strtol((const char *)ch, NULL, 16);
	}
}

void
EllipticsProxy::deleteHandler(fastcgi::Request *request) {
	if (elliptics_node_->state_num() < state_num_) {
		request->setStatus(403);
		return;
	}

	std::string filename = request->hasArg("name") ? request->getArg("name") :
		request->getScriptName().substr(sizeof ("/delete/") - 1, std::string::npos);

	std::vector<int> groups;
	if (!metabase_write_addr_.empty() && !metabase_read_addr_.empty()) {
		try {
			groups = getMetaInfo(filename);
		}
		catch (...) {
			groups = getGroups(request);
		}
	}
	else {
		groups = getGroups(request);
	}

	try {
		elliptics_node_->set_groups(groups);
		if (request->hasArg("id")) {
			int error = -1;
			struct dnet_id id;
			memset(&id, 0, sizeof(id));

			dnet_parse_numeric_id(request->getArg("id"), id);

			for (size_t i = 0; i < groups.size(); ++i) {
				id.group_id = groups[i];
				try {
					elliptics_node_->remove(id);
					error = 0;
				} catch (const std::exception &e) {
					log()->error("can not delete file %s in group %d: %s", filename.c_str(), groups[i], e.what());
				}
			}

			if (error) {
				std::ostringstream str;
				str << dnet_dump_id(&id) << ": REMOVE failed";
				throw std::runtime_error(str.str());
			}
		} else {
			elliptics_node_->remove(filename);
		}
		elliptics_node_->set_groups(groups_);
		request->setStatus(200);
	}
	catch (const std::exception &e) {
		log()->error("can not delete file %s %s", filename.c_str(), e.what());
		request->setStatus(503);
	}
	catch (...) {
		log()->error("can not write file %s", filename.c_str());
		request->setStatus(504);
	}
}

void
EllipticsProxy::bulkReadHandler(fastcgi::Request *request) {

	typedef boost::char_separator<char> Separator;
	typedef boost::tokenizer<Separator> Tokenizer;

	std::vector<int> groups;
	groups = getGroups(request);

	std::string content_type = "application/octet";

	try {
		unsigned int aflags = request->hasArg("aflags") ? boost::lexical_cast<unsigned int>(request->getArg("aflags")) : 0;
		//unsigned int ioflags = request->hasArg("ioflags") ? boost::lexical_cast<unsigned int>(request->getArg("ioflags")) : 0;
		//int column = request->hasArg("column") ? boost::lexical_cast<int>(request->getArg("column")) : 0;
		int group_id = request->hasArg("group_id") ? boost::lexical_cast<int>(request->getArg("group_id")) : 0;
		std::string key_type = request->hasArg("key_type") ? request->getArg("key_type") : "name";

		if (group_id <= 0) {
			std::ostringstream str;
			str << "BULK_READ failed: group_id is mandatory and it should be > 0";
			throw std::runtime_error(str.str());
		}

		std::vector<std::string> ret;

		if (key_type.compare("name") == 0) {
			// body of POST request contains lines with file names
			std::vector<std::string> keys;

			// First, concatenate it
			std::string content;
			request->requestBody().toString(content);

			/*content.reserve(buffer.size());
			for (fastcgi::DataBuffer::SegmentIterator it = buffer.begin(), end = buffer.end(); it != end; ++it) {
				std::pair<char*, boost::uint64_t> chunk = *it;
				content.append(chunk.first, chunk.second);
			}*

			// Then parse to extract key names
			Separator sep(" \n");
			Tokenizer tok(content, sep);

			for (Tokenizer::iterator it = tok.begin(), end = tok.end(); end != it; ++it) {
				log()->debug("BULK_READ: adding key %s", it->c_str());
				keys.push_back(*it);
			}

			// Finally, call bulk_read method
			ret = elliptics_node_->bulk_read(keys, group_id, aflags);

		} else {
			std::ostringstream str;
			str << "BULK_READ failed: unsupported key type " << key_type;
			throw std::runtime_error(str.str());
		}

		if (ret.size() == 0) {
			std::ostringstream str;
			str << "BULK_READ failed: zero size reply";
			throw std::runtime_error(str.str());
		}

		std::string result;

		for (size_t i = 0; i < ret.size(); ++i) {
			result += ret[i];
		}

		request->setStatus(200);
		request->setContentType(content_type);
		request->setHeader("Content-Length", boost::lexical_cast<std::string>(result.length()));
		request->write(result.data(), result.size());
	}
	catch (const std::exception &e) {
		log()->error("BULK_READ failed: %s", e.what());
		request->setStatus(404);
	}
	catch (...) {
		log()->error("BULK_READ failed");
		request->setStatus(404);
	}
}

struct bulk_file_info_single {
	std::string addr;
	std::string path;
	int group;
	int status;
};

struct bulk_file_info {
	char id[2 * DNET_ID_SIZE + 1];
	char crc[2 * DNET_ID_SIZE + 1];
	boost::uint64_t size;

	std::vector<struct bulk_file_info_single> groups;
};
	

void
EllipticsProxy::bulkWriteHandler(fastcgi::Request *request) {

	std::vector<int> groups;
	groups = getGroups(request);

	std::string content_type = "application/octet";

	try {
		unsigned int aflags = request->hasArg("aflags") ? boost::lexical_cast<unsigned int>(request->getArg("aflags")) : 0;
		unsigned int ioflags = request->hasArg("ioflags") ? boost::lexical_cast<unsigned int>(request->getArg("ioflags")) : 0;
		int column = request->hasArg("column") ? boost::lexical_cast<int>(request->getArg("column")) : 0;
		int group_id = request->hasArg("group_id") ? boost::lexical_cast<int>(request->getArg("group_id")) : 0;
		std::string key_type = request->hasArg("key_type") ? request->getArg("key_type") : "id";

		if (group_id <= 0) {
			std::ostringstream str;
			str << "BULK_WRITE failed: group_id is mandatory and it should be > 0";
			throw std::runtime_error(str.str());
		}

		std::string lookup;
		std::ostringstream ostr;
		std::map<std::string, struct bulk_file_info> results;

		if (key_type.compare("id") == 0) {

			std::vector<struct dnet_io_attr> ios;
			struct dnet_io_attr io;
			std::vector<std::string> data;
			std::string empty;
			boost::uint64_t pos = 0, data_readed;

			fastcgi::DataBuffer buffer = request->requestBody();

			struct dnet_id row;
			std::string id;

			while (pos < buffer.size()) {
				memset(&io, 0, sizeof(io));
				io.flags = ioflags;
				io.type = column;

				data_readed = buffer.read(pos, (char *)io.id, DNET_ID_SIZE);
				if (data_readed != DNET_ID_SIZE) {
					std::ostringstream str;
					str << "BULK_WRITE failed: read " << data_readed << ", " << DNET_ID_SIZE << " bytes needed ";
					throw std::runtime_error(str.str());
				}
				pos += DNET_ID_SIZE;

				data_readed = buffer.read(pos, (char *)&io.size, sizeof(io.size));
				if (data_readed != sizeof(io.size)) {
					std::ostringstream str;
					str << "BULK_WRITE failed: read " << data_readed << ", " << sizeof(io.size) << " bytes needed ";
					throw std::runtime_error(str.str());
				}
				if (io.size > (buffer.size() - pos)) {
					std::ostringstream str;
					str << dnet_dump_id_str(io.id) << ": BULK_WRITE failed: size of data is " << io.size 
						<< " but buffer size is " << (buffer.size() - pos);
				}
				pos += sizeof(io.size);

				boost::scoped_array<char> tmp_buffer(new char[io.size]);
				data_readed = buffer.read(pos, tmp_buffer.get(), io.size);
				if (data_readed != io.size) {
					std::ostringstream str;
					str << dnet_dump_id_str(io.id) << ": BULK_WRITE failed: read " << data_readed << ", " << io.size << " bytes needed ";
					throw std::runtime_error(str.str());
				}
				pos += io.size;

				ios.push_back(io);
				data.push_back(empty);
				data.back().assign(tmp_buffer.get(), io.size);

				struct bulk_file_info file_info;

				elliptics_node_->transform(tmp_buffer.get(), row);
				dnet_dump_id_len_raw(io.id, DNET_ID_SIZE, file_info.id);
				dnet_dump_id_len_raw(row.id, DNET_ID_SIZE, file_info.crc);
				file_info.size = io.size;

				id.assign(file_info.id);
				results[id] = file_info;
			}

			lookup = elliptics_node_->bulk_write(ios, data, aflags);

		} else {
			std::ostringstream str;
			str << "BULK_WRITE failed: unsupported key type " << key_type;
			throw std::runtime_error(str.str());
		}


		ostr << "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n";

		long size = lookup.size();
		char *data = (char *)lookup.data();
		long min_size = sizeof(struct dnet_cmd) +
				sizeof(struct dnet_addr) +
				sizeof(struct dnet_attr) +
				sizeof(struct dnet_addr_attr) +
				sizeof(struct dnet_file_info);

		char addr_dst[512];
		char id_str[2 * DNET_ID_SIZE + 1];
		struct bulk_file_info_single file_info;
		std::string id;

		while (size > min_size) {
			struct dnet_addr *addr = (struct dnet_addr *)data;
			struct dnet_cmd *cmd = (struct dnet_cmd *)(addr + 1);
			struct dnet_attr *attr = (struct dnet_attr *)(cmd + 1);
			struct dnet_addr_attr *a = (struct dnet_addr_attr *)(attr + 1);

			struct dnet_file_info *info = (struct dnet_file_info *)(a + 1);
			dnet_convert_file_info(info);

			dnet_server_convert_dnet_addr_raw(addr, addr_dst, sizeof (addr_dst) - 1);

			dnet_dump_id_len_raw(cmd->id.id, DNET_ID_SIZE, id_str);
			id.assign(id_str);

			file_info.addr.assign(addr_dst);
			file_info.path.assign((char *)(info + 1));
			file_info.group = cmd->id.group_id;
			file_info.status = cmd->status;

			results[id].groups.push_back(file_info);


			data += min_size + info->flen;
			size -= min_size + info->flen;
		}
		lookup.clear();

		for (std::map<std::string, struct bulk_file_info>::iterator it = results.begin(); it != results.end(); it++) {
			ostr << "<post obj=\"\" id=\"" << it->first <<
				"\" crc=\"" << it->second.crc << "\" groups=\"" << groups.size() <<
				"\" size=\"" << it->second.size << "\">\n" <<
				"	<written>\n";

			for (std::vector<struct bulk_file_info_single>::iterator gr_it = it->second.groups.begin();
					gr_it != it->second.groups.end(); gr_it++) {
				ostr << "		<complete addr=\"" << gr_it->addr << "\" path=\"" <<
					gr_it->path << "\" group=\"" << gr_it->group <<
					"\" status=\"" << gr_it->status << "\"/>\n";
			}
			ostr << "	</written>\n</post>\n";
		}

		std::string result = ostr.str();

		request->setStatus(200);
		request->setContentType(content_type);
		request->setHeader("Content-Length", boost::lexical_cast<std::string>(result.length()));
		request->write(result.data(), result.size());
	}
	catch (const std::exception &e) {
		log()->error("BULK_WRITE failed: %s", e.what());
		request->setStatus(404);
	}
	catch (...) {
		log()->error("BULK_WRITE failed");
		request->setStatus(404);
	}
}

void
EllipticsProxy::execScriptHandler(fastcgi::Request *request) {
	if (elliptics_node_->state_num() < state_num_) {
		request->setStatus(403);
		return;
	}

	struct dnet_id id;
	memset(&id, 0, sizeof(id));

	std::string filename = request->hasArg("name") ? request->getArg("name") :
		request->getScriptName().substr(sizeof ("/exec-script/") - 1, std::string::npos);

	if (request->hasArg("id")) {
		dnet_parse_numeric_id(request->getArg("id"), id);
	} else {
		elliptics_node_->transform(filename, id);
	}

	std::string script = request->hasArg("script") ? request->getArg("script") : "";

	std::vector<int> groups;
	groups = getGroups(request);
	std::string ret;

	try {
		log()->debug("script is <%s>", script.c_str());

		elliptics_node_->set_groups(groups);

		std::string content;
		request->requestBody().toString(content);


		if (script.empty()) {
			ret = elliptics_node_->exec_name(&id, "", content, "", DNET_EXEC_PYTHON);
		} else {
			ret = elliptics_node_->exec_name(&id, script, "", content, DNET_EXEC_PYTHON_SCRIPT_NAME);
		}

		elliptics_node_->set_groups(groups_);

	}
	catch (const std::exception &e) {
		log()->error("can not execute script %s %s", script.c_str(), e.what());
		request->setStatus(503);
	}
	catch (...) {
		log()->error("can not execute script %s", script.c_str());
		request->setStatus(503);
	}
	request->setStatus(200);
	request->write(ret.data(), ret.size());
}
*/

std::vector<int>
EllipticsProxy::getGroups(Key &key, const std::vector<int> &groups, int count) const {
	std::vector<int> lgroups;

	if (groups.size()) {
		lgroups = groups;
	}
	else {
		lgroups = groups_;
#ifdef HAVE_METABASE
		if (!metabase_write_addr_.empty() && !metabase_read_addr_.empty()) {
			try {
				lgroups = getMetaInfo(key);
			}
			catch (...) {
			}
		}
#endif /* HAVE_METABASE */
		if (lgroups.size() > 1) {
			std::vector<int>::iterator git = lgroups.begin();
			++git;
			std::random_shuffle(git, lgroups.end());
		}
	}

	if (count != 0 && count < (int)(lgroups.size())) {
		lgroups.erase(lgroups.begin() + count, lgroups.end());
	}

	if (!lgroups.size()) {
		throw std::runtime_error("There is no groups");
	}

	return lgroups;
}


#ifdef HAVE_METABASE
void
EllipticsProxy::uploadMetaInfo(const std::vector<int> &groups, const Key &key) const {
	try {
		std::string url = metabase_write_addr_ + "/groups-meta.";
		if (key.byId()) {
			url += key.id().dump() + ".id";
		} else {
			url += key.filename() + ".file";
		}

		yandex::common::curl_wrapper<yandex::common::boost_threading_traits> curl(url);
		curl.header("Expect", "");
		curl.timeout(10);

		std::string result;
		for (std::size_t i = 0; i < groups.size(); ++i) {
			if (!result.empty()) {
				result += ':';
			}
			result += boost::lexical_cast<std::string>(groups[i]);
		}

		std::stringstream response;
		long status = curl.perform_post(response, result);
		if (200 != status) {
			throw std::runtime_error("Failed to get meta info");
		}
	}
	catch (...) {
		throw;
	}
}

std::vector<int>
EllipticsProxy::getMetaInfo(const Key &key) const {
	try {
		std::stringstream response;
		long status;

		std::string url = metabase_read_addr_ + "/groups-meta.";
		if (key.byId()) {
			url += key.id().dump() + ".id";
		} else {
			url += key.filename() + ".file";
		}

		yandex::common::curl_wrapper<yandex::common::boost_threading_traits> curl(url);
		curl.header("Expect", "");
		curl.timeout(10);
		status = curl.perform(response);

		if (200 != status) {
			throw std::runtime_error("Failed to get meta info");
		}

		std::vector<int> groups;

		Separator sep(":");
		Tokenizer tok(response.str(), sep);

		for (Tokenizer::iterator it = tok.begin(), end = tok.end(); end != it; ++it) {
			groups.push_back(boost::lexical_cast<int>(*it));
		}

		return groups;
	}
	catch (...) {
		throw;
	}
}

std::vector<int> EllipticsProxy::get_metabalancer_groups_impl(uint64_t count, uint64_t size, Key &key)
{
	return mastermind_->get_metabalancer_groups(count);
}

GroupInfoResponse EllipticsProxy::get_metabalancer_group_info_impl(int group)
{
	auto aux = mastermind_->get_metabalancer_group_info(group);
	GroupInfoResponse res;

	res.nodes = aux.nodes;
	res.couples = aux.couples;
	res.status = aux.status;

	return res;
}

std::map<int, std::vector<int> > EllipticsProxy::get_symmetric_groups() {
	return mastermind_->get_symmetric_groups();
}

std::vector<std::vector<int> > EllipticsProxy::get_bad_groups() {
	return mastermind_->get_bad_groups();
}

std::vector<int> EllipticsProxy::get_all_groups() {
	return mastermind_->get_all_groups();
}
#endif /* HAVE_METABASE */

void
EllipticsProxy::transform(Key &key)
{
	session elliptics_session(*elliptics_node_);
	key.transform(elliptics_session);
}

/*
#ifdef HAVE_METABASE
std::vector<int> 
EllipticsProxy::getMetabaseGroups(fastcgi::Request *request, size_t count, struct dnet_id &id) {
	std::vector<int> groups;
	int rc = 0;

	log()->debug("Requesting metabase for %d groups for upload, metabase_usage: %d", count, metabase_usage_);

	if (metabase_usage_ == PROXY_META_NORMAL && request->hasArg("groups")) {
		groups = getGroups(request, count);
		count = groups.size();
		return groups;
	}

	if (count <= 0)
		return groups;

	if (!metabase_socket_.get())
		return groups;

	MetabaseRequest req;
	req.groups_num = count;
	req.stamp = ++metabase_current_stamp_;
	req.id.assign(id.id, id.id+DNET_ID_SIZE);

	msgpack::sbuffer buf;
	msgpack::pack(buf, req);

	zmq::message_t empty_msg;
	zmq::message_t req_msg(buf.size());
	memcpy(req_msg.data(), buf.data(), buf.size());

	try {
		if (!metabase_socket_->send(empty_msg, ZMQ_SNDMORE)) {
			log()->error("error during zmq send empty");
			return groups;
		}
		if (!metabase_socket_->send(req_msg)) {
			log()->error("error during zmq send");
			return groups;
		}
	} catch(const zmq::error_t& e) {
		log()->error("error during zmq send: %s", e.what());
		return groups;
	}

	zmq::pollitem_t items[] = {
		{ *metabase_socket_, 0, ZMQ_POLLIN, 0 }
	};

	rc = 0;

	try {
		rc = zmq::poll(items, 1, metabase_timeout_);
	} catch(const zmq::error_t& e) {
		log()->error("error during zmq poll: %s", e.what());
		return groups;
	} catch (...) {
		log()->error("error during zmq poll");
		return groups;
	}

	if (rc == 0) {
		log()->error("error: no answer from zmq");
	} else if (items[0].revents && ZMQ_POLLIN) {
		zmq::message_t msg;
		MetabaseResponse resp;
		msgpack::unpacked unpacked;

		try {
			resp.stamp = 0;
			while (resp.stamp < metabase_current_stamp_) {
				if (!metabase_socket_->recv(&msg, ZMQ_NOBLOCK)) {
					break;
				}
				if (!metabase_socket_->recv(&msg, ZMQ_NOBLOCK)) {
					break;
				}

				msgpack::unpack(&unpacked, static_cast<const char*>(msg.data()), msg.size());
				unpacked.get().convert(&resp);
				log()->debug("current stamp is %d", resp.stamp);
			}
		} catch(const msgpack::unpack_error &e) {
			log()->error("error during msgpack::unpack: %s", e.what());
			return groups;
		} catch(...) {
			log()->error("error during msgpack::unpack");
			return groups;
		}

		return resp.groups;
	} else {
		log()->error("error after zmq poll: %d", rc);
	}

	return groups;
}
#endif /* HAVE_METABASE *

size_t
EllipticsProxy::paramsNum(Tokenizer &tok) {
	size_t result = 0;
	for (Tokenizer::iterator it = ++tok.begin(), end = tok.end(); end != it; ++it) {
		++result;
	}
	return result;
}

FCGIDAEMON_REGISTER_FACTORIES_BEGIN()
FCGIDAEMON_ADD_DEFAULT_FACTORY("elliptics-proxy", EllipticsProxy);
FCGIDAEMON_REGISTER_FACTORIES_END()

*/
} // namespace elliptics

