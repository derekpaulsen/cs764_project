#pragma once
#include "BTreeOLC.h"
#include<atomic>
#include<memory>
#include<array>
#include<shared_mutex>
#include<utility>
#include<optional>
#include<algorithm>
#include "omp.h"

using namespace btreeolc;


template<class K, class V>
class RingBufferedBTree : public BTree<K, Versioned<V>> {
	public:
		static constexpr int max_threads = 32;
		static constexpr int capacity_per_thread = 256;


	struct InsertBuffer {
		static constexpr long capacity = 1024;
		std::shared_mutex mu;
		std::atomic<long> pos, min_version;
		std::array<std::pair<K,Versioned<V>>, capacity> buf;
		
		InsertBuffer() : mu(), buf(), pos(0), min_version(0) {
		}

		bool push_back(K key, V val, std::atomic<long> *version) {
			long insert_pos = pos++;
			if (insert_pos < capacity) {
				buf[insert_pos].first = key;
				buf[insert_pos].second.val = val;
				buf[insert_pos].second.version = version->fetch_add(1);
				return false;
			} else {
				return true;
			}
		}

		bool search(K key, Versioned<V> &result, const long max_version) {
			bool found = false;
			int end = std::min(pos.load(), capacity);
			long min_version = this->min_version.load();
			for (int i = 0; i < end; ++i) {
				if (buf[i].first == key &&
						buf[i].second.version <= max_version &&
						buf[i].second.version >= min_version) {
					
					result.set(buf[i].second);
					found = true;
				}
			}
			return found;
		}

		void reset(const long version) {
			min_version = version;
			pos = 0;
		}
							
	};


	private:
		std::atomic<InsertBuffer *>insert_buffer;
		std::array<InsertBuffer *, max_threads> last_insert_buffer;
		std::array<InsertBuffer, max_threads> insert_buffers;
		std::atomic<long> version;

	public:

		RingBufferedBTree() : version(1) {
			BTree<K,Versioned<V>>();
			last_insert_buffer.fill(nullptr);
			insert_buffer = insert_buffers.data();
			for (auto &buf : insert_buffers)
				buf.reset(0);
		}
		
		void insert(K key, V payload) {
			int tnum = omp_get_thread_num();

			start_insert:
			InsertBuffer *curr_buffer;
			// grab the next valid buffer		
			while (!(curr_buffer = this->insert_buffer.load()))
				insert_buffer.wait(nullptr);

			if (curr_buffer != last_insert_buffer[tnum]) {
				if (last_insert_buffer[tnum]) {
					// unlock the last buffer that was inserted into
					last_insert_buffer[tnum]->mu.unlock_shared();
					last_insert_buffer[tnum] = nullptr;
				}
			
				// check if this thread has a lock on the current buffer
				// try to lock the current buffer
				if (curr_buffer->mu.try_lock_shared()) {
					// if successful, update the last buffer inserted into
					last_insert_buffer[tnum] = curr_buffer;
				} else {
					// if not successful restart insert
					goto start_insert;
				}
			}
			if (curr_buffer->push_back(key, payload, &version)) {

				if (last_insert_buffer[tnum]) {
					last_insert_buffer[tnum]->mu.unlock_shared();
					last_insert_buffer[tnum] = nullptr;
				}

				if (insert_buffer.compare_exchange_strong(curr_buffer, nullptr)) {
					// this thread has to insert everything 
					//
					// find next open insert buffer
					while (true) {
						for (auto &buf : insert_buffers) {
							if ((&buf != curr_buffer) && (buf.mu.try_lock())) {
								insert_buffer = &buf;
								buf.mu.unlock();
								goto loop_done;
							}
						}
					}

					loop_done:
					insert_buffer.notify_all();
					//wait for other threads to complete inserting
					curr_buffer->mu.lock();

					for (const auto &p : curr_buffer->buf) {
						BTree<K,Versioned<V>>::insert(p.first, p.second);
					}

					curr_buffer->reset(version.load());
					curr_buffer->mu.unlock();

				} 
				Versioned<V> vpayload (payload, version++);
				// insert into buffer failed, directly insert instead
				BTree<K,Versioned<V>>::insert(key, vpayload);
			}
			// if the buffer has been swapped, unlock the last buffer that
			// was inserted into
			if (last_insert_buffer[tnum] && last_insert_buffer[tnum] != insert_buffer.load()) {
				// unlock the last buffer that was inserted into
				last_insert_buffer[tnum]->mu.unlock_shared();
				last_insert_buffer[tnum] = nullptr;
			}
			
		}
		
		bool lookup(const K key, V &result) {
			// FIXME this is incorrect;
			bool found = false;
			Versioned<V> vres, r;
			vres.version = -1;
			r.version = -1;

			long curr_version = version.load();
			auto *insert_buf = insert_buffer.load();
			if (insert_buf && insert_buf->search(key, vres, curr_version))
				found = true;

			for (auto &buf : insert_buffers) {
				if (buf.search(key, r, curr_version)) {
						vres.set(r);
						found = true;
				}
			}

			if (BTree<K,Versioned<V>>::lookup(key, r) && r.version <= curr_version) {
				vres.set(r);
				found = true;
			}
			if (found) {
				result = vres.val;
				return true;
			} else {
				return false;
			}
		}

		void release_locks() {
			int tnum = omp_get_thread_num();

			if (last_insert_buffer[tnum]) {
				// unlock the last buffer that was inserted into
				last_insert_buffer[tnum]->mu.unlock_shared();
				last_insert_buffer[tnum] = nullptr;
			}
		}
};
