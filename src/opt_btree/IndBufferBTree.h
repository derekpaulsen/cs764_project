#pragma once
#include "BTreeOLC.h"
#include<atomic>
#include<array>
#include<shared_mutex>
#include<utility>
#include<optional>
#include "omp.h"

using namespace btreeolc;



template<class K, class V>
class IndBufferedBTree : public BTree<K, V> {
	public:
		static constexpr int capacity = 12;

	struct alignas(64) Buffer {
		static constexpr int capacity = 63;
		long size = 0;
		std::array<std::pair<K,V>, capacity> buf;
		
		bool is_full() const {
			return size == capacity;
		}
		bool is_empty() const {
			return size == 0;
		}

		const std::pair<K,V> *begin() const{
			return &buf[0];
		}
		const std::pair<K,V> *end() const {
			return &buf[size];
		}

		void push_back(K key, V val) {
			buf[size].first = key;
			buf[size].second = val;
			++size;
		}

		std::pair<K, V> &operator[](const int pos) { 
			return buf[pos];
		}

		bool search(K key, V &result) const {
			long sz = size;

			if (!sz) 
				return false;

			for (int i = 0; i < sz; ++i) {
				if (buf[i].first == key) {
					result = buf[i].second;
					return true;
				}
			}
			return false;
		}

	};

	struct InsertBuffer {
		std::shared_mutex mu;
		std::array<Buffer, capacity> thread_bufs ;
		
		InsertBuffer() : mu() {
			thread_bufs.fill(Buffer());
		}

		bool push_back(K key, V val, int thread_num) {
			auto &buf = thread_bufs[thread_num];
			buf.push_back(key, val);
			return buf.is_full();
		}

		bool search(K key, V &result) {
			for (const auto &buf : thread_bufs) {
				if (buf.search(key, result))
					return true;
			}

			return false;
		}
							
	};

	private:
		std::atomic<InsertBuffer *>insert_buffer;
		std::array<InsertBuffer *, capacity> last_insert_buffer;

	public:

		IndBufferedBTree() : insert_buffer(new InsertBuffer()) {
			BTree<K,V>();
			last_insert_buffer.fill(nullptr);
		}
		
		void insert(K key, V payload) {
			int tnum = omp_get_thread_num();
			start_insert:
			InsertBuffer *curr_buffer = nullptr;
			// grab the next valid buffer		
			while (!(curr_buffer = this->insert_buffer.load()))
				insert_buffer.wait(nullptr);
			
			// check if this thread has a lock on the current buffer
			if (curr_buffer != last_insert_buffer[tnum]) {
				// unlock the last buffer that was inserted into
				if (last_insert_buffer[tnum]) {
					last_insert_buffer[tnum]->mu.unlock_shared();
					last_insert_buffer[tnum] = nullptr;
				}
				// try to lock the current buffer
				if (curr_buffer->mu.try_lock_shared()) 
					// if successful, update the last buffer inserted into
					last_insert_buffer[tnum] = curr_buffer;
				 else {
					// if not successful restart insert
					last_insert_buffer[tnum] = nullptr;
					goto start_insert;
				}
			}

			if (curr_buffer->push_back(key, payload, tnum)) {
				// this thread's buffer is full
				curr_buffer->mu.unlock_shared();
				last_insert_buffer[tnum] = nullptr;

				if (insert_buffer.compare_exchange_strong(curr_buffer, nullptr)) {
					// this thread has to insert everything 
					insert_buffer = new InsertBuffer();
					insert_buffer.notify_all();
					//wait for other threads to complete inserting
					curr_buffer->mu.lock();

					for (const auto &buf : curr_buffer->thread_bufs) {
						for (const auto &p : buf) {
							BTree<K,V>::insert(p.first, p.second);
						}
					}
					curr_buffer->mu.unlock();
					delete curr_buffer;
					curr_buffer = nullptr;
				} 				
			}
			// if the buffer has been swapped, unlock the last buffer that
			// was inserted into
			if (curr_buffer && (curr_buffer != insert_buffer.load())) {
					curr_buffer->mu.unlock_shared();
					last_insert_buffer[tnum] = nullptr;
			}
			
		}
		
		bool lookup(const K key, V &result) {
			// FIXME this is incorrect
			if (insert_buffer.load()->search(key, result))
				return true;
			else 
				return BTree<K,V>::lookup(key, result);

		}
};
