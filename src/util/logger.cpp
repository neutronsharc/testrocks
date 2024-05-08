//
// Created by shawn.ouyang on 12/07/2023.
//

#include "spdlog/pattern_formatter.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_sinks.h"
#include "util/logger.h"

/**
 * It's supposed that, each software module in the codebase uses its own Logger object.
 */
const char *LogModule::name[MAX_LOGGER_COUNT + 1] = {
  "general",
  NULL
};

/**
 * Overload custom flag formatter to show thread name in log message.
 */
class thread_name_flag : public spdlog::custom_flag_formatter {
public:
  void format(const spdlog::details::log_msg&, const std::tm&, spdlog::memory_buf_t& dest) override {
    static thread_local std::string tname;  // each thread has its own thread name.
    if (tname.empty()) {
      char buffer[64];
      pthread_getname_np(pthread_self(), buffer, sizeof(buffer) - 1);
      tname = std::string(buffer);
    }
    dest.append(tname.data(), tname.data() + tname.size());
  }

  std::unique_ptr<custom_flag_formatter> clone() const override {
    return spdlog::details::make_unique<thread_name_flag>();
  }
};


Logger *Logger::AllocateLoggers() {
  auto loggers = static_cast<Logger *>(malloc(sizeof(Logger) * LogModule::MAX_LOGGER_COUNT));
  for (int i = 0; i < LogModule::MAX_LOGGER_COUNT; ++i) {
    new(&loggers[i])Logger(LogModule::name[i]);
  }
  return loggers;
}

bool Logger::Init(const std::string &filename, int size_mb, int num_files, bool to_stderr, bool to_file) {
  try {
    if (!to_stderr && !to_file) {
      std::cerr << "Logger: no output!\n";
      exit(-1);
    }
    // use sync mode, so that flush will work

    std::vector<spdlog::sink_ptr> sinks;
    if (to_stderr) {
      sinks.push_back(std::make_shared<spdlog::sinks::stdout_sink_mt>());
    }
    if (to_file) {
      sinks.push_back(
        std::make_shared<spdlog::sinks::rotating_file_sink_mt>(filename, size_mb * 1024L * 1024, num_files));
    }
    spd_logger_ = std::make_shared<spdlog::logger>(log_name_, begin(sinks), end(sinks));
    // %t: thread id,  %q: thread name
    auto formatter = std::make_unique<spdlog::pattern_formatter>();
    formatter->add_flag<thread_name_flag>('q').set_pattern("[%Y-%m-%d %H:%M:%S.%f%z][%l][%t %q]%v");
    spd_logger_->set_formatter(std::move(formatter));
    SetLogLevel(LogLevel::INFO);
  } catch (const spdlog::spdlog_ex& ex) {
    std::cerr << "Logger: " << ex.what() << std::endl;
    return false;
  }
  log_file_name_ = filename;
  return true;
}

Logger::~Logger() {
  LOG_INFO("tear down logger {}", log_name_);
  if (spd_logger_) {
    spd_logger_->flush();
    spd_logger_.reset();
  }
}

std::string getOsName() {
#ifdef _WIN32
  return "Windows 32-bit";
#elif _WIN64
  return "Windows 64-bit";
#elif __APPLE__ || __MACH__
  return "Mac OSX";
#elif __linux__
  return "Linux";
#elif __FreeBSD__
  return "FreeBSD";
#elif __unix || __unix__
    return "Unix";
#else
    return "Other";
#endif
}

void InitializeLogger(const std::string& base_logfile) {
  if (base_logfile.empty()) {
    std::cerr << "must have valid base logfile name" << std::endl;
    exit(-1);
  }
  int logfile_num = 5;
  int logfile_sizeMB = 10;

  // Create the general Logger.
  bool to_file = true;
  bool to_stderr = true;
  auto logfile = base_logfile + "/general.log";
  std::cout << "logfile=" << logfile
            << ", num of logfiles=" << logfile_num
            << ", logfile_sizeMB=" << logfile_sizeMB << std::endl;
  Logger::GetLogger(LogModule::GENERAL)->Init(logfile,
                                              logfile_sizeMB,
                                              logfile_num,
                                              to_stderr,
                                              to_file);

  // (todo) Create more Logger, one for each software module.
}
