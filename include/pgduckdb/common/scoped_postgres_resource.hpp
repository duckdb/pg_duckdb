#pragma once

#include <functional>
#include <type_traits>
#include <utility>

namespace pgduckdb {

template <class T>
class ScopedPostgresResource {
public:
	using resource_destructor_t = std::function<void(T)>;

	explicit ScopedPostgresResource(T resource, resource_destructor_t &&destructor)
	    : resource(resource), destructor(std::move(destructor)) {
	}

	~ScopedPostgresResource() {
		destructor(resource);
	}

	// General case
	T
	get() const {
		return resource;
	}

	// Enable operator-> if T is a pointer
	typename std::enable_if<std::is_pointer<T>::value, T>::type
	operator->() const {
		return resource;
	}

private:
	T resource;
	resource_destructor_t destructor;
};

// Specialization for pointer types
template <class T>
class ScopedPostgresResource<T *> {
public:
	using resource_destructor_t = std::function<void(T *)>;

	explicit ScopedPostgresResource(T *resource, resource_destructor_t &&destructor)
	    : resource(resource), destructor(std::move(destructor)) {
	}

	~ScopedPostgresResource() {
		destructor(resource);
	}

	T *
	get() const {
		return resource;
	}

	T *
	operator->() const {
		return resource;
	}

	// Cast to void*
	operator void *() const {
		return static_cast<void *>(resource);
	}

private:
	T *resource;
	resource_destructor_t destructor;
};

} // namespace pgduckdb
