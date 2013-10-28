#ifndef SRC__LOGGERS_HPP
#define SRC__LOGGERS_HPP

#include <cocaine/framework/logging.hpp>
#include <elliptics/cppdef.h>

class cocaine_logger_t : public cocaine::framework::logger_t {
public:
	cocaine_logger_t(const ioremap::elliptics::logger &logger)
		: m_logger(logger)
	{}

	void emit(cocaine::logging::priorities priority, const std::string& message) {
		int lvl = level(priority);
		m_logger.log(lvl, message.c_str());
	}

	cocaine::logging::priorities verbosity() const {
		using namespace cocaine::logging;
		switch(m_logger.get_log_level()) {
		case DNET_LOG_DATA:
			return priorities::ignore;
		case DNET_LOG_ERROR:
			return priorities::error;
		case DNET_LOG_INFO:
			return priorities::info;
		case DNET_LOG_NOTICE:
			return priorities::info;
		default:
			return priorities::debug;
		}
	}

private:
	int level(cocaine::logging::priorities priority) {
		using namespace cocaine::logging;
		switch(priority) {
		case priorities::ignore:
			return DNET_LOG_DATA;
		case priorities::error:
			return DNET_LOG_ERROR;
		case priorities::warning:
			return DNET_LOG_ERROR;
		case priorities::info:
			return DNET_LOG_INFO;
		default:
			return DNET_LOG_DEBUG;
		}
	}

	mutable ioremap::elliptics::logger m_logger;
};

#endif /* SRC__LOGGERS_HPP */
