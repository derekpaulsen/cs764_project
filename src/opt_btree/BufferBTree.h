#pragma once
#include "BTreeOLC.h"
#include<atomic>
#include<shared_mutex>
#include<utility>
#include<optional>

using namespace btreeolc;


template<class K, class V>
class BufferedBTree : public BTree<K, V> {
	
	struct State {
		BTreeLeaf<K,V> *leaf;
		int pos, low_key;

		bool operator==(const State &other) const {
			return leaf == other.leaf && pos == other.pos && low_key == other.low_key;
		}
		State operator+(const int i) const {
			return {leaf, pos+i, low_key};
		}
	};
	private:
		std::atomic<int> insert_count;
		std::atomic<State> state;
		std::shared_mutex leaf_lock;
		
		
		BTreeLeaf<K,V> *allocate_new_leaf() {
			auto leaf = new BTreeLeaf<K,V>();
			leaf->count = 0;
			// clear the memory 
			//std::memset(leaf->keys, 0, sizeof(BTreeLeaf<K,V>::keys));
			//std::memset(leaf->payloads, 0, sizeof(BTreeLeaf<K,V>::payloads));
			return leaf;
		}
		

	public:
		// 75% load factor on bulk inserted leaves
		static const int max_inserts = BTreeLeaf<K,V>::maxEntries * .75;
		
		BufferedBTree() : state(State(allocate_new_leaf(), 0, -1)) {
			this->root = nullptr;
		}

		
		void insert(K key, V payload) {
			start_insert:
			auto cs = state.load();
			if (key > cs.low_key) {
				//if (current_low_key != low_key.load())
				//	goto start_insert;

				
				while(!state.compare_exchange_strong(cs, cs +1)) {
					if (key <= cs.low_key || cs.pos > max_inserts)
						goto start_insert;
				}
				
				int current_pos = cs.pos;
				auto *current_leaf = cs.leaf;

				if (current_pos < max_inserts) {
					current_leaf->insert_unordered(key, payload, current_pos);
					++insert_count;

				} else if (current_pos == max_inserts) {
					current_leaf->insert_unordered(key, payload, current_pos);
					++insert_count;
					
					while(insert_count != max_inserts + 1);

					current_leaf->count = max_inserts + 1;
					K high_key = current_leaf->sort_and_dedupe();
					insert_leaf(current_leaf);


					insert_count = 0;
					state = {allocate_new_leaf(), 0, high_key};
					state.notify_all();


				} else {
					// wait for new leaf to be allocated
					state.wait(cs);
					goto start_insert;
				}
			} else {
				// insert normally into the tree
				BTree<K, V>::insert(key, payload);
				//std::cerr << "i " << key << '\n';
			}
		}
		
		bool lookup(const K key, V &result) {
			State cs = state.load();
			auto *current_leaf = cs.leaf;
			const K current_low_key = cs.low_key;

			if (key <= current_low_key) {
				return BTree<K,V>::lookup(key, result);
			} 
			auto count = cs.pos + 1;
			auto res = current_leaf->search_unsorted(key, count, result);
			//TODO what happens when the leaf is split?
			if (current_leaf != state.load().leaf) {
				// leaf was inserted into the tree
				// wait for insert to complete
				state.wait(cs);
				bool needRestart;
				// try reading the leaf
				do {
					needRestart = false;
					uint64_t versionNode = current_leaf->readLockOrRestart(needRestart);
					if (needRestart)
						continue;

					int pos = current_leaf->lowerBound(key);
					if ((pos<current_leaf->count) && (current_leaf->keys[pos]==key)) {
						result = current_leaf->payloads[pos];
						current_leaf->readUnlockOrRestart(versionNode, needRestart);
						if (needRestart)
							continue;
						else
							return true;

					} else {
						// read failed on leaf, might have been split
						// try normal read
						return BTree<K,V>::lookup(key, result);
					}

				} while(needRestart);

			}	
			return res;
		}

		void insert_leaf(BTreeLeaf<K, V> *new_leaf) {
			NodeBase *null = nullptr;
			if (this->root.compare_exchange_strong(null, new_leaf)) {
				return;
			}
		
			K k = new_leaf->keys[new_leaf->count-1];
			int restartCount = 0;
			bool leaf_inserted = false;
			restart:
			if (restartCount++)
				this->yield(restartCount);
			bool needRestart = false;

			// Current node
			NodeBase* node = this->root;
			uint64_t versionNode = node->readLockOrRestart(needRestart);
			if (needRestart || (node!=this->root)) goto restart;

			// Parent of current node
			BTreeInner<K>* parent = nullptr;
			uint64_t versionParent;

			while (node->type==PageType::BTreeInner) {
				auto inner = static_cast<BTreeInner<K>*>(node);

				// Split eagerly if full
				if (inner->isFull()) {
					// Lock
					if (parent) {
						parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
						if (needRestart) goto restart;
					}
					node->upgradeToWriteLockOrRestart(versionNode, needRestart);
					if (needRestart) {
						if (parent)
							parent->writeUnlock();
						goto restart;
					}
					if (!parent && (node != this->root)) { // there's a new parent
						node->writeUnlock();
						goto restart;
					}
					// Split
					K sep; BTreeInner<K>* newInner = inner->split(sep);
					if (parent)
						parent->insert(sep,newInner);
					else
						this->makeRoot(sep,inner,newInner);
					// Unlock and restart
					node->writeUnlock();
					if (parent)
						parent->writeUnlock();
					goto restart;
				}

				if (parent) {
					parent->readUnlockOrRestart(versionParent, needRestart);
					if (needRestart) goto restart;
				}

				parent = inner;
				versionParent = versionNode;

				node = inner->children[inner->lowerBound(k)];
				inner->checkOrRestart(versionNode, needRestart);
				if (needRestart) goto restart;
				versionNode = node->readLockOrRestart(needRestart);
				if (needRestart) goto restart;
			}

			// Split leaf if full
			if (!leaf_inserted) {
				// Lock
				if (parent) {
					parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
					if (needRestart) goto restart;
				}
				node->upgradeToWriteLockOrRestart(versionNode, needRestart);
				if (needRestart) {
					if (parent) parent->writeUnlock();
					goto restart;
				}
				if (!parent && (node != this->root)) { // there's a new parent
					node->writeUnlock();
					goto restart;
				}
				auto leaf = static_cast<BTreeLeaf<K,V>*>(node);
				if (parent)
					parent->insert(leaf->keys[leaf->count-1], new_leaf);
				else
					this->makeRoot(leaf->keys[leaf->count-1], leaf, new_leaf);
				// Unlock and restart
				node->writeUnlock();
				if (parent)
					parent->writeUnlock();
				// signal that the leaf has been inserted
				leaf_inserted = true;
				goto restart;
			} else {
				return; // success
			}
		}

};

