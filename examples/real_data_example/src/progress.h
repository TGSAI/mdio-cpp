#ifndef REAL_DATA_EXAMPLE_PROGRESS_H_
#define REAL_DATA_EXAMPLE_PROGRESS_H_

#include <mdio/mdio.h>

#include <chrono>
#include <indicators/cursor_control.hpp>
#include <indicators/indeterminate_progress_bar.hpp>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

namespace mdio {

template <typename T>
class ProgressTracker {
 public:
  explicit ProgressTracker(const std::string& message = "Loading data...")
      : message_(message),
        bar_{
            indicators::option::BarWidth{40},
            indicators::option::Start{"["},
            indicators::option::Fill{"·"},
            indicators::option::Lead{"<==>"},
            indicators::option::End{"]"},
            indicators::option::PostfixText{message},
            indicators::option::ForegroundColor{indicators::Color::yellow},
            indicators::option::FontStyles{std::vector<indicators::FontStyle>{
                indicators::FontStyle::bold}},
        } {
    std::cout << "\033[2K\r" << std::flush;
    indicators::show_console_cursor(false);
    start_time_ = std::chrono::steady_clock::now();
  }

  void tick() {
    auto current_time = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       current_time - start_time_)
                       .count();
    std::stringstream time_str;
    time_str << message_ << " " << elapsed << "s";
    bar_.set_option(indicators::option::PostfixText{time_str.str()});
    bar_.tick();
  }

  void complete() {
    bar_.mark_as_completed();
    bar_.set_option(
        indicators::option::ForegroundColor{indicators::Color::green});
    bar_.set_option(indicators::option::PostfixText{message_ + " completed"});
    indicators::show_console_cursor(true);
  }

  ~ProgressTracker() { indicators::show_console_cursor(true); }

 private:
  std::string message_;
  std::chrono::steady_clock::time_point start_time_;
  indicators::IndeterminateProgressBar bar_;
};

template <typename T = void, DimensionIndex R = dynamic_rank,
          ArrayOriginKind OriginKind = offset_origin>
Future<VariableData<T, R, OriginKind>> ReadWithProgress(
    Variable<T>& variable, const std::string& message = "Loading data...") {
  auto tracker = std::make_shared<ProgressTracker<T>>(message);
  auto future = variable.Read();

  std::thread([tracker, future]() {
    while (!future.ready()) {
      tracker->tick();
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    tracker->complete();
  }).detach();

  return future;
}

}  // namespace mdio

#endif  // REAL_DATA_EXAMPLE_PROGRESS_H_