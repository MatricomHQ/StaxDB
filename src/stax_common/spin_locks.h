#pragma once

#include <atomic>
#include <thread> 
#include <limits> 
#include <mutex>  

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h> 
#define USE_MM_PAUSE
#endif


class SpinLock {
    std::atomic_flag lock_flag = ATOMIC_FLAG_INIT;

public:
    inline void lock() noexcept {
        while (lock_flag.test_and_set(std::memory_order_acquire)) {
#ifdef USE_MM_PAUSE
            _mm_pause();
#else
            std::this_thread::yield();
#endif
        }
    }

    inline bool try_lock() noexcept {
        return !lock_flag.test_and_set(std::memory_order_acquire);
    }

    inline void unlock() noexcept {
        lock_flag.clear(std::memory_order_release);
    }

    SpinLock(const SpinLock&) = delete;
    SpinLock& operator=(const SpinLock&) = delete;
    SpinLock(SpinLock&&) = delete;
    SpinLock& operator=(SpinLock&&) = delete; 
    SpinLock() = default;
};


class UniqueSpinLockGuard {
    SpinLock& lock_;
    bool owns_ = false;

public:
    explicit UniqueSpinLockGuard(SpinLock& lock) : lock_(lock) {
        lock_.lock();
        owns_ = true;
    }

    explicit UniqueSpinLockGuard(SpinLock& lock, std::try_to_lock_t) : lock_(lock) {
        owns_ = lock_.try_lock();
    }

    ~UniqueSpinLockGuard() {
        if (owns_) {
            lock_.unlock();
        }
    }

    bool owns_lock() const noexcept { return owns_; }
    explicit operator bool() const noexcept { return owns_lock(); }

    UniqueSpinLockGuard(const UniqueSpinLockGuard&) = delete;
    UniqueSpinLockGuard& operator=(const UniqueSpinLockGuard&) = delete;
    UniqueSpinLockGuard(UniqueSpinLockGuard&&) = delete;
    UniqueSpinLockGuard& operator=(UniqueSpinLockGuard&&) = delete;
};


class SharedSpinLock {
    std::atomic<size_t> state_ = {};

    static constexpr size_t WRITER_BIT = 1ULL;
    static constexpr size_t READER_INCREMENT = 2ULL;

public:
    inline void lock() noexcept {
        size_t current_state;
        for (;;) {
            while (state_.load(std::memory_order_relaxed) & WRITER_BIT) {
#ifdef USE_MM_PAUSE
                _mm_pause();
#else
                std::this_thread::yield();
#endif
            }
            
            current_state = state_.load(std::memory_order_relaxed);
            if (!(current_state & WRITER_BIT)) {
                 if (state_.compare_exchange_weak(current_state, current_state | WRITER_BIT, std::memory_order_acquire, std::memory_order_relaxed)) {
                    while ((state_.load(std::memory_order_relaxed) & ~WRITER_BIT) != 0) {
#ifdef USE_MM_PAUSE
                        _mm_pause();
#else
                        std::this_thread::yield();
#endif
                    }
                    return;
                }
            }
#ifdef USE_MM_PAUSE
            _mm_pause();
#else
            std::this_thread::yield();
#endif
        }
    }

    inline bool try_lock() noexcept {
        size_t expected_state = 0;
        return state_.compare_exchange_strong(expected_state, WRITER_BIT, std::memory_order_acquire, std::memory_order_relaxed);
    }

    inline void unlock() noexcept {
        state_.fetch_and(~WRITER_BIT, std::memory_order_release);
    }

    inline void lock_shared() noexcept {
        for (;;) {
            size_t current_state = state_.load(std::memory_order_relaxed);
            if (!(current_state & WRITER_BIT)) {
                if (state_.compare_exchange_weak(current_state, current_state + READER_INCREMENT, std::memory_order_acquire, std::memory_order_relaxed)) {
                    return;
                }
            }
#ifdef USE_MM_PAUSE
            _mm_pause();
#else
            std::this_thread::yield();
#endif
        }
    }

    inline bool try_lock_shared() noexcept {
        size_t current_state = state_.load(std::memory_order_relaxed);
        if (current_state & WRITER_BIT) {
            return false;
        }
        return state_.compare_exchange_strong(current_state, current_state + READER_INCREMENT, std::memory_order_acquire, std::memory_order_relaxed);
    }

    inline void unlock_shared() noexcept {
        state_.fetch_sub(READER_INCREMENT, std::memory_order_release);
    }
    
    SharedSpinLock(SharedSpinLock&& other) noexcept : state_(other.state_.load(std::memory_order_relaxed)) {
        other.state_.store(0, std::memory_order_relaxed);
    }
    SharedSpinLock& operator=(SharedSpinLock&& other) noexcept {
        if (this != &other) {
            state_.store(other.state_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            other.state_.store(0, std::memory_order_relaxed);
        }
        return *this;
    }

    SharedSpinLock(const SharedSpinLock&) = delete;
    SharedSpinLock& operator=(const SharedSpinLock&) = delete;
    SharedSpinLock() = default;
};


class UniqueSharedSpinLockGuard {
    SharedSpinLock& lock_;
    bool owns_ = false;

public:
    explicit UniqueSharedSpinLockGuard(SharedSpinLock& lock) : lock_(lock) {
        lock_.lock();
        owns_ = true;
    }

    explicit UniqueSharedSpinLockGuard(SharedSpinLock& lock, std::try_to_lock_t) : lock_(lock) {
        owns_ = lock_.try_lock();
    }
    
    ~UniqueSharedSpinLockGuard() {
        if (owns_) {
            lock_.unlock();
        }
    }

    bool owns_lock() const noexcept { return owns_; }
    explicit operator bool() const noexcept { return owns_lock(); }

    UniqueSharedSpinLockGuard(const UniqueSharedSpinLockGuard&) = delete;
    UniqueSharedSpinLockGuard& operator=(const UniqueSharedSpinLockGuard&) = delete;
    UniqueSharedSpinLockGuard(UniqueSharedSpinLockGuard&&) = delete;
    UniqueSharedSpinLockGuard& operator=(UniqueSharedSpinLockGuard&&) = delete;
};


class SharedSharedSpinLockGuard {
    SharedSpinLock& lock_;
    bool owns_ = false;

public:
    explicit SharedSharedSpinLockGuard(SharedSpinLock& lock) : lock_(lock) {
        lock_.lock_shared();
        owns_ = true;
    }

    explicit SharedSharedSpinLockGuard(SharedSpinLock& lock, std::try_to_lock_t) : lock_(lock) {
        owns_ = lock_.try_lock_shared();
    }

    ~SharedSharedSpinLockGuard() {
        if (owns_) {
            lock_.unlock_shared();
        }
    }

    bool owns_lock() const noexcept { return owns_; }
    explicit operator bool() const noexcept { return owns_lock(); }

    SharedSharedSpinLockGuard(const SharedSharedSpinLockGuard&) = delete;
    SharedSharedSpinLockGuard& operator=(const SharedSharedSpinLockGuard&) = delete;
    SharedSharedSpinLockGuard(SharedSharedSpinLockGuard&&) = delete;
    SharedSharedSpinLockGuard& operator=(SharedSharedSpinLockGuard&&) = delete;
};