#pragma once
#include "BTreeOLC.h"
#include<atomic>
#include<mutex>
#include<utility>
#include<optional>

using namespace btreeolc;


template<class K, class V>
class RingBufferBTree : public BTree<K, V> {
	
	public:
		// 75% load factor on bulk inserted leaves
		//static constexpr int max_inserts = BTreeLeaf<K,V>::maxEntries * .75;
		static constexpr long insert_chunk_size = 64; // 2^5
		static constexpr long insert_chunk_mask = 0x0000003F;
		static constexpr long buffer_power = 12;
		static constexpr long buffer_size = 1 << buffer_power; // 2^11
		static constexpr long buffer_mask = 0xFFFFFFFFFFFFFFFF >> (64 - buffer_power);

	private:
		std::atomic<long> size, end;
		std::pair<K,V> buffer [buffer_size];
		
	public:
		
		RingBufferBTree() : size(0), end(0), BTree<K,V>() {
		}

		
		void insert(K key, V payload) {
			long current_end = end++;
			long pos = current_end & buffer_mask;
			buffer[pos].first = key;
			buffer[pos].second = payload;
			++size;
			if (current_end && !(current_end & insert_chunk_mask)) {
				long insert_pos = (current_end - insert_chunk_size) & buffer_mask;
				for (int i = 0; i < insert_chunk_size; ++i) {
					BTree<K,V>::insert(buffer[insert_pos].first, buffer[insert_pos].second);
					++insert_pos;
				}
				size -= insert_chunk_size;
			}

		}
		
		bool lookup(const K key, V &result) {
			if (!BTree<K,V>::lookup(key, result)) {
				long current_size = size.load();
				long pos = (end.load() - current_size) & buffer_mask;
				for (int i = 0; i < current_size; ++i) {

					if (buffer[pos & buffer_mask].first == key) {
						result = buffer[pos & buffer_mask].second;
						return true;
					}
					++pos;
				}
			} else {
				return true;
			}
			return false;

		}

};

