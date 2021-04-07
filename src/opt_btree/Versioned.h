#pragma once

template<class T>
struct Versioned {
	T val;
	long version;

	Versioned() = default;
	Versioned(const T v, const long ver) : val(v), version(ver) {}
	Versioned(const Versioned<T> &other) = default;
	Versioned(Versioned<T> &&other) = default;
	Versioned<T> &operator=(const Versioned<T> &other) = default;

	void set(const Versioned<T> &other) {
		if (other.version > this->version) {
			val = other.val;
			version = other.version;
		} 	
	}

	void operator=(const Versioned<T> &&other) = delete;
			

};
