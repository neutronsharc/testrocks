//
// Created by shawn.ouyang on 12/07/2023.
//

#ifndef TESTROCKS_LOGGER_H
#define TESTROCKS_LOGGER_H


#if __GNUC__ >= 7
#include <iostream>
#endif

#include <cassert>
#include <memory>
#include <string>
#include <vector>

#include "spdlog/spdlog.h"
#include "macros.h"


#ifndef __FILENAME__
#define __FILENAME__ strrchr(__FILE__, '/') + 1
#endif

typedef enum LogLevel {
  TRACE = 0,
  DEBUG = 1,
  INFO = 2,
  WARN = 3,
  ERROR = 4,
  CRITICAL = 5,
  OFF = 6
} LogLevel;

// It's possible different modules in the code use different Logger to log message.
// There will be an array of Loggers, each Logger used by one code module.
struct LogModule {
  enum type {
    GENERAL = 0,
    MAX_LOGGER_COUNT
  };
  static const char *name[MAX_LOGGER_COUNT + 1];
};

// Colors of the log print.
#ifndef NO_COLOR
#define NO_COLOR(fmt) fmt
#endif

#ifndef GREEN_COLOR
#define GREEN_COLOR(fmt) "\033[1;32m" fmt "\033[0m"
#endif

#ifndef BLUE_COLOR
#define BLUE_COLOR(fmt) "\033[1;34m" fmt "\033[0m"
#endif

#ifndef YELLOW_COLOR
#define YELLOW_COLOR(fmt) "\033[1;33m" fmt "\033[0m"
#endif

#ifndef RED_COLOR
#define RED_COLOR(fmt) "\033[1;31m" fmt "\033[0m"
#endif

#ifndef PURPLE_COLOR
#define PURPLE_COLOR(fmt) "\033[1;35m" fmt "\033[0m"
#endif

/**
 * Get the number of argument of the spdlog format string
 * @param str fomart.
 * @return the number of argument.
 */
constexpr int GetLogArgumentNumber(const char* str) {
  return ((str[0] == '\0') ? 0 :
          ((str[0] == '{' && (str[1] == '}' || (str[1] >= '0' && str[1] < 9) || str[1] == ':'))
           ? 1 + GetLogArgumentNumber(str + 2)
           : GetLogArgumentNumber(str + 1)));
}

/**
 * A wrapper class to provide access to spdlog. To use the Logger:
 *
 */
class Logger {
 public:
  /**
   * Class method to create Loggers.
   * The design idea is that, there is an array of Logger objects, and each module
   * in the codebase uses its own Logger object.
   * @param type
   * @return
   */
  static inline Logger* GetLogger(LogModule::type type = LogModule::GENERAL) {
    // AllocateLoggers() is called only once to allocate an array of loggers.
    static Logger* loggers = AllocateLoggers();
    return loggers + type;
  }

  /**
   * Initialize a logger.
   * @param filename full path log file name, such as "/var/log/myproject/module_a/logfile".
   * @param size_mb  max size of each log file in MB.
   * @param num_files max number of log files to keep.
   * @param to_stderr  whether to log to stderr.
   * @param to_file    whether to log to file.
   * @return  true if the logger is ready to use.
   *
   * After successfully return, the log level will be set to INFO
   */
  bool Init(const std::string& filename, int size_mb, int num_files, bool to_stderr, bool to_file);

  ~Logger();

  inline spdlog::logger* GetSpdlogger() { return spd_logger_.get(); }
  inline void SetLogLevel(LogLevel level) { log_level_ = level; }
  inline LogLevel GetLogLevel() { return log_level_; }

 private:
  // Deny public access to ctor, because Logger can only be created at program start.
  explicit Logger(std::string log_name) :
    log_level_(INFO), log_name_(std::move(log_name)) {}

  Logger() = default;

  // Create all loggers defined in struct LogModule.
  // It's supposed that, each module in the codebase uses its own Logger object.
  static Logger* AllocateLoggers();

  LogLevel log_level_;  // current log level
  std::string log_name_;       // name of this Logger
  std::string log_file_name_;  // filename used by this Logger.

  std::shared_ptr<spdlog::logger> spd_logger_;   // Spd logger that does actual logging.
};

extern std::string getOsName();

extern void InitializeLogger(const std::string& base_logfile);

#define LOG_MACRO(module, fmt, log_level, likely_to_print, log_fun, color, do_flush, ...)\
do {\
  auto log = Logger::GetLogger(module);\
  if (likely_to_print(log->GetLogLevel() <= log_level)) {\
    auto spdlog = log->GetSpdlogger();\
    if (likely(spdlog != nullptr)) {\
      static_assert(GetLogArgumentNumber(fmt) == COUNT(1, ##__VA_ARGS__) - 1,\
       "number of arguments mismatch with format");\
      spdlog->log_fun(color(fmt) " [{}:{}:{}]", ##__VA_ARGS__, __FILENAME__, __func__, __LINE__);\
      if (do_flush) {\
        spdlog->flush();\
      }\
    } else {\
      fprintf(stderr, "W: logger not initialized [%s:%s:%d]\n", __FILENAME__, __func__, __LINE__);\
    }\
  }\
} while (false)

// info log, default not to flush after print.
#define LOG_INFO2(module, fmt, ...)                                           \
LOG_MACRO(module, fmt, LogLevel::INFO, likely, info, NO_COLOR, false, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...) LOG_INFO2(LogModule::GENERAL, fmt,  ##__VA_ARGS__)

// error log
#define LOG_ERROR2(module, fmt, ...)                                           \
LOG_MACRO(module, fmt, LogLevel::ERROR, likely, info, RED_COLOR, false, ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) LOG_ERROR2(LogModule::GENERAL, fmt,  ##__VA_ARGS__)

#define ABORT(fmt, ...)\
do {\
  LOG_ERROR(fmt, ##__VA_ARGS__);\
  std::abort();\
} while (false)

#define ASSERT(condition, fmt, ...)\
if (unlikely(!(condition))) {\
  ABORT("condition(" #condition ") failed: " fmt, ##__VA_ARGS__);\
}

#define LOG_FLUSH2(module)\
do {\
  auto spdlog = Logger::GetLogger(module)->GetSpdlogger();\
  if (likely(spdlog != nullptr)) {\
    spdlog->flush();\
  }\
} while (false)

#define LOG_FLUSH() LOG_FLUSH2(LogModule::GENERAL)


#endif // TESTROCKS_LOGGER_H
