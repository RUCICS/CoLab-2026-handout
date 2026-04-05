#pragma once

#include <cstddef>
#include <iterator>
#include <span>

#include "schedlab/task_view.hpp"

namespace schedlab {

class QueueView {
public:
  using SizeFn = std::size_t (*)(const void*) noexcept;
  using GetFn = const TaskView& (*)(const void*, std::size_t) noexcept;

  QueueView() = default;
  explicit QueueView(std::span<const TaskView* const> tasks) : tasks_(tasks) {}
  QueueView(const void* context, SizeFn size_fn, GetFn get_fn)
      : context_(context), size_fn_(size_fn), get_fn_(get_fn) {}

  [[nodiscard]] std::size_t size() const noexcept {
    return context_ != nullptr ? size_fn_(context_) : tasks_.size();
  }
  [[nodiscard]] bool empty() const noexcept {
    return size() == 0;
  }

  [[nodiscard]] const TaskView& operator[](std::size_t index) const noexcept {
    if (context_ != nullptr) {
      return get_fn_(context_, index);
    }
    return *tasks_[index];
  }

  [[nodiscard]] const TaskView& front() const noexcept {
    return (*this)[0];
  }
  [[nodiscard]] const TaskView& back() const noexcept {
    return (*this)[size() - 1];
  }

  class Iterator {
  public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = TaskView;
    using difference_type = std::ptrdiff_t;
    using pointer = const TaskView*;
    using reference = const TaskView&;

    Iterator() = default;
    Iterator(const QueueView* owner, std::size_t index) noexcept : owner_(owner), index_(index) {}

    reference operator*() const noexcept {
      return (*owner_)[index_];
    }
    pointer operator->() const noexcept {
      return &(*owner_)[index_];
    }

    Iterator& operator++() noexcept {
      ++index_;
      return *this;
    }

    Iterator operator++(int) noexcept {
      Iterator copy(*this);
      ++index_;
      return copy;
    }

    friend bool operator==(const Iterator& lhs, const Iterator& rhs) noexcept {
      return lhs.owner_ == rhs.owner_ && lhs.index_ == rhs.index_;
    }

    friend bool operator!=(const Iterator& lhs, const Iterator& rhs) noexcept {
      return !(lhs == rhs);
    }

  private:
    const QueueView* owner_ = nullptr;
    std::size_t index_ = 0;
  };

  [[nodiscard]] Iterator begin() const noexcept {
    return Iterator(this, 0);
  }
  [[nodiscard]] Iterator end() const noexcept {
    return Iterator(this, size());
  }

private:
  std::span<const TaskView* const> tasks_{};
  const void* context_ = nullptr;
  SizeFn size_fn_ = nullptr;
  GetFn get_fn_ = nullptr;
};

} // namespace schedlab
