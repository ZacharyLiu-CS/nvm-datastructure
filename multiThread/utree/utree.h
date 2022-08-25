#pragma once
#include <cassert>
#include <climits>
#include <fstream>
#include <future>
#include <iostream>
#define USE_PMDK
#ifdef USE_PMDK
#include <libpmemobj.h>
#endif
#include <gperftools/profiler.h>
#include <math.h>
#include <mutex>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <vector>

#define PAGESIZE 512
#define CACHE_LINE_SIZE 64
#define IS_FORWARD(c) (c % 2 == 0)

using entry_key_t = int64_t;

static pthread_mutex_t print_mtx;

const uint64_t SPACE_PER_THREAD = 35ULL * 1024ULL * 1024ULL * 1024ULL;
const uint64_t SPACE_OF_MAIN_THREAD = 35ULL * 1024ULL * 1024ULL * 1024ULL;
extern __thread char *start_addr;
extern __thread char *curr_addr;

using namespace std;

inline void mfence() { asm volatile("mfence" ::: "memory"); }

inline void clflush(char *data, int len) {
  volatile char *ptr = (char *)((unsigned long)data & ~(CACHE_LINE_SIZE - 1));
  mfence();
  for (; ptr < data + len; ptr += CACHE_LINE_SIZE) {
    asm volatile(".byte 0x66; clflush %0" : "+m"(*(volatile char *)ptr));
  }
  mfence();
}

struct list_node_t {
  uint64_t ptr;
  entry_key_t key;
  bool isUpdate;
  bool isDelete;
  struct list_node_t *next;
  void printAll(void) {
    printf("addr=%p, key=%ld, ptr=%lu, isUpdate=%d, isDelete=%d, next=%p\n",
           this, this->key, this->ptr, this->isUpdate, this->isDelete,
           this->next);
  }
};

#ifdef USE_PMDK
POBJ_LAYOUT_BEGIN(btree);
POBJ_LAYOUT_TOID(btree, list_node_t);
POBJ_LAYOUT_END(btree);
static PMEMobjpool *pop;
#endif
inline void *alloc(size_t size) {
#ifdef USE_PMDK
  TOID(list_node_t) p;
  POBJ_ZALLOC(pop, &p, list_node_t, size);
  return pmemobj_direct(p.oid);
#else
  void *ret = curr_addr;
  memset(ret, 0, sizeof(list_node_t));
  curr_addr += size;
  if (curr_addr >= start_addr + SPACE_PER_THREAD) {
    printf("start_addr is %p, curr_addr is %p, SPACE_PER_THREAD is %lu, no "
           "free space to alloc\n",
           start_addr, curr_addr, SPACE_PER_THREAD);
    exit(0);
  }
  return ret;
#endif
}

class page;

class btree {
private:
  int height;
  char *root;

public:
  list_node_t *list_head = NULL;
  btree(std::string db_path = "/mnt/pmem0/tmp-utree",
        uint64_t pool_size = 40ULL << 30);
  ~btree();
  void setNewRoot(char *);
  void getNumberOfNodes();
  void btree_insert_pred(entry_key_t, char *, char **pred, bool *);
  void btree_insert_internal(char *, entry_key_t, char *, uint32_t);
  void btree_delete(entry_key_t);
  char *btree_search(entry_key_t);
  char *btree_search_pred(entry_key_t, bool *f, char **, bool debug = false);
  void printAll();
  void insert(entry_key_t, char *); // Insert
  void remove(entry_key_t);         // Remove
  char *search(entry_key_t);        // Search

  void print() {
    int i = 0;
    list_node_t *tmp = list_head;
    while (tmp->next != NULL) {
      // printf("%d-%d\t", tmp->next->key, tmp->next->ptr);
      tmp = tmp->next;
      printf("node=%d, ", i);
      tmp->printAll();
      i++;
    }
    printf("\n");
  }
  friend class page;
};

class header {
private:
  page *leftmost_ptr;     // 8 bytes
  page *sibling_ptr;      // 8 bytes
  page *pred_ptr;         // 8 bytes
  uint32_t level;         // 4 bytes
  uint8_t switch_counter; // 1 bytes
  uint8_t is_deleted;     // 1 bytes
  int16_t last_index;     // 2 bytes
  std::mutex *mtx;        // 8 bytes

  friend class page;
  friend class btree;

public:
  header() {
    mtx = new std::mutex();

    leftmost_ptr = NULL;
    sibling_ptr = NULL;
    pred_ptr = NULL;
    switch_counter = 0;
    last_index = -1;
    is_deleted = false;
  }

  ~header() { delete mtx; }
};

class entry {
private:
  entry_key_t key; // 8 bytes
  char *ptr;       // 8 bytes

public:
  entry() {
    key = LONG_MAX;
    ptr = NULL;
  }

  friend class page;
  friend class btree;
};

const int cardinality = (PAGESIZE - sizeof(header)) / sizeof(entry);
const int count_in_line = CACHE_LINE_SIZE / sizeof(entry);

class page {
private:
  header hdr;                 // header in persistent memory, 16 bytes
  entry records[cardinality]; // slots in persistent memory, 16 bytes * n

public:
  friend class btree;

  page(uint32_t level = 0) {
    hdr.level = level;
    records[0].ptr = NULL;
  }

  // this is called when tree grows
  page(page *left, entry_key_t key, page *right, uint32_t level = 0) {
    hdr.leftmost_ptr = left;
    hdr.level = level;
    records[0].key = key;
    records[0].ptr = (char *)right;
    records[1].ptr = NULL;

    hdr.last_index = 0;
  }

  void *operator new(size_t size) {
    void *ret;
    int res = posix_memalign(&ret, 64, size);
    return ret;
  }

  inline int count() {
    uint8_t previous_switch_counter;
    int count = 0;
    do {
      previous_switch_counter = hdr.switch_counter;
      count = hdr.last_index + 1;

      while (count >= 0 && records[count].ptr != NULL) {
        if (IS_FORWARD(previous_switch_counter))
          ++count;
        else
          --count;
      }

      if (count < 0) {
        count = 0;
        while (records[count].ptr != NULL) {
          ++count;
        }
      }

    } while (previous_switch_counter != hdr.switch_counter);

    return count;
  }

  inline bool remove_key(entry_key_t key) {
    // Set the switch_counter
    if (IS_FORWARD(hdr.switch_counter))
      ++hdr.switch_counter;

    bool shift = false;
    int i;
    for (i = 0; records[i].ptr != NULL; ++i) {
      if (!shift && records[i].key == key) {
        records[i].ptr =
            (i == 0) ? (char *)hdr.leftmost_ptr : records[i - 1].ptr;
        shift = true;
      }

      if (shift) {
        records[i].key = records[i + 1].key;
        records[i].ptr = records[i + 1].ptr;
      }
    }

    if (shift) {
      --hdr.last_index;
    }
    return shift;
  }

  bool remove(btree *bt, entry_key_t key, bool only_rebalance = false,
              bool with_lock = true) {
    hdr.mtx->lock();

    bool ret = remove_key(key);

    hdr.mtx->unlock();

    return ret;
  }

  inline void insert_key(entry_key_t key, char *ptr, int *num_entries,
                         bool flush = true, bool update_last_index = true) {
    // update switch_counter
    if (!IS_FORWARD(hdr.switch_counter))
      ++hdr.switch_counter;

    // FAST
    if (*num_entries == 0) { // this page is empty
      entry *new_entry = (entry *)&records[0];
      entry *array_end = (entry *)&records[1];
      new_entry->key = (entry_key_t)key;
      new_entry->ptr = (char *)ptr;

      array_end->ptr = (char *)NULL;

    } else {
      int i = *num_entries - 1, inserted = 0;
      records[*num_entries + 1].ptr = records[*num_entries].ptr;

      // FAST
      for (i = *num_entries - 1; i >= 0; i--) {
        if (key < records[i].key) {
          records[i + 1].ptr = records[i].ptr;
          records[i + 1].key = records[i].key;
        } else {
          records[i + 1].ptr = records[i].ptr;
          records[i + 1].key = key;
          records[i + 1].ptr = ptr;
          inserted = 1;
          break;
        }
      }
      if (inserted == 0) {
        records[0].ptr = (char *)hdr.leftmost_ptr;
        records[0].key = key;
        records[0].ptr = ptr;
      }
    }

    if (update_last_index) {
      hdr.last_index = *num_entries;
    }
    ++(*num_entries);
  }

  // Insert a new key - FAST and FAIR
  page *store(btree *bt, char *left, entry_key_t key, char *right, bool flush,
              bool with_lock, page *invalid_sibling = NULL) {
    if (with_lock) {
      hdr.mtx->lock(); // Lock the write lock
    }
    if (hdr.is_deleted) {
      if (with_lock) {
        hdr.mtx->unlock();
      }

      return NULL;
    }

    int num_entries = count();

    for (int i = 0; i < num_entries; i++)
      if (key == records[i].key) {
        records[i].ptr = right;
        if (with_lock)
          hdr.mtx->unlock();
        return this;
      }

    // If this node has a sibling node,
    if (hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling)) {
      // Compare this key with the first key of the sibling
      if (key > hdr.sibling_ptr->records[0].key) {
        if (with_lock) {
          hdr.mtx->unlock(); // Unlock the write lock
        }
        return hdr.sibling_ptr->store(bt, NULL, key, right, true, with_lock,
                                      invalid_sibling);
      }
    }

    // FAST
    if (num_entries < cardinality - 1) {
      insert_key(key, right, &num_entries, flush);

      if (with_lock) {
        hdr.mtx->unlock(); // Unlock the write lock
      }

      return this;
    } else { // FAIR
      // overflow
      // create a new node
      page *sibling = new page(hdr.level);
      int m = (int)ceil(num_entries / 2);
      entry_key_t split_key = records[m].key;

      // migrate half of keys into the sibling
      int sibling_cnt = 0;
      if (hdr.leftmost_ptr == NULL) { // leaf node
        for (int i = m; i < num_entries; ++i) {
          sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt,
                              false);
        }
      } else { // internal node
        for (int i = m + 1; i < num_entries; ++i) {
          sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt,
                              false);
        }
        sibling->hdr.leftmost_ptr = (page *)records[m].ptr;
      }

      sibling->hdr.sibling_ptr = hdr.sibling_ptr;
      sibling->hdr.pred_ptr = this;
      if (sibling->hdr.sibling_ptr != NULL)
        sibling->hdr.sibling_ptr->hdr.pred_ptr = sibling;
      hdr.sibling_ptr = sibling;

      // set to NULL
      if (IS_FORWARD(hdr.switch_counter))
        hdr.switch_counter += 2;
      else
        ++hdr.switch_counter;
      records[m].ptr = NULL;
      hdr.last_index = m - 1;
      num_entries = hdr.last_index + 1;

      page *ret;

      // insert the key
      if (key < split_key) {
        insert_key(key, right, &num_entries);
        ret = this;
      } else {
        sibling->insert_key(key, right, &sibling_cnt);
        ret = sibling;
      }

      // Set a new root or insert the split key to the parent
      if (bt->root == (char *)this) { // only one node can update the root ptr
        page *new_root =
            new page((page *)this, split_key, sibling, hdr.level + 1);
        bt->setNewRoot((char *)new_root);

        if (with_lock) {
          hdr.mtx->unlock(); // Unlock the write lock
        }
      } else {
        if (with_lock) {
          hdr.mtx->unlock(); // Unlock the write lock
        }
        bt->btree_insert_internal(NULL, split_key, (char *)sibling,
                                  hdr.level + 1);
      }

      return ret;
    }
  }
  // revised
  inline void insert_key(entry_key_t key, char *ptr, int *num_entries,
                         char **pred, bool flush = true,
                         bool update_last_index = true) {
    // update switch_counter
    if (!IS_FORWARD(hdr.switch_counter))
      ++hdr.switch_counter;

    // FAST
    if (*num_entries == 0) { // this page is empty
      entry *new_entry = (entry *)&records[0];
      entry *array_end = (entry *)&records[1];
      new_entry->key = (entry_key_t)key;
      new_entry->ptr = (char *)ptr;

      array_end->ptr = (char *)NULL;

      if (hdr.pred_ptr != NULL)
        *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
    } else {
      int i = *num_entries - 1, inserted = 0;
      records[*num_entries + 1].ptr = records[*num_entries].ptr;

      // FAST
      for (i = *num_entries - 1; i >= 0; i--) {
        if (key < records[i].key) {
          records[i + 1].ptr = records[i].ptr;
          records[i + 1].key = records[i].key;
        } else {
          records[i + 1].ptr = records[i].ptr;
          records[i + 1].key = key;
          records[i + 1].ptr = ptr;
          *pred = records[i].ptr;
          inserted = 1;
          break;
        }
      }
      if (inserted == 0) {
        records[0].ptr = (char *)hdr.leftmost_ptr;
        records[0].key = key;
        records[0].ptr = ptr;
        if (hdr.pred_ptr != NULL)
          *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
      }
    }

    if (update_last_index) {
      hdr.last_index = *num_entries;
    }
    ++(*num_entries);
  }

  // revised
  // Insert a new key - FAST and FAIR
  /********
   * if key exists, return NULL
   */
  page *store(btree *bt, char *left, entry_key_t key, char *right, bool flush,
              bool with_lock, char **pred, page *invalid_sibling = NULL) {
    if (with_lock) {
      hdr.mtx->lock(); // Lock the write lock
    }
    if (hdr.is_deleted) {
      if (with_lock) {
        hdr.mtx->unlock();
      }
      return NULL;
    }

    int num_entries = count();

    for (int i = 0; i < num_entries; i++)
      if (key == records[i].key) {
        // Already exists, we don't need to do anything, just return.
        *pred = records[i].ptr;
        if (with_lock)
          hdr.mtx->unlock();
        return NULL;
      }

    // If this node has a sibling node,
    if (hdr.sibling_ptr && (hdr.sibling_ptr != invalid_sibling)) {
      // Compare this key with the first key of the sibling
      if (key > hdr.sibling_ptr->records[0].key) {
        if (with_lock) {
          hdr.mtx->unlock(); // Unlock the write lock
        }
        return hdr.sibling_ptr->store(bt, NULL, key, right, true, with_lock,
                                      pred, invalid_sibling);
      }
    }

    // FAST
    if (num_entries < cardinality - 1) {
      insert_key(key, right, &num_entries, pred);

      if (with_lock) {
        hdr.mtx->unlock(); // Unlock the write lock
      }

      return this;
    } else { // FAIR
      // overflow
      // create a new node
      page *sibling = new page(hdr.level);
      int m = (int)ceil(num_entries / 2);
      entry_key_t split_key = records[m].key;

      // migrate half of keys into the sibling
      int sibling_cnt = 0;
      if (hdr.leftmost_ptr == NULL) { // leaf node
        for (int i = m; i < num_entries; ++i) {
          sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt,
                              false);
        }
      } else { // internal node
        for (int i = m + 1; i < num_entries; ++i) {
          sibling->insert_key(records[i].key, records[i].ptr, &sibling_cnt,
                              false);
        }
        sibling->hdr.leftmost_ptr =
            (page *)records[m].ptr; //记录左侧叶子节点中最大key的ptr
      }

      //将分裂出的b+tree节点插入原节点右侧
      sibling->hdr.sibling_ptr = hdr.sibling_ptr;
      sibling->hdr.pred_ptr = this;
      if (sibling->hdr.sibling_ptr != NULL)
        sibling->hdr.sibling_ptr->hdr.pred_ptr = sibling;
      hdr.sibling_ptr = sibling;

      // set to NULL
      if (IS_FORWARD(hdr.switch_counter))
        hdr.switch_counter += 2;
      else
        ++hdr.switch_counter;
      records[m].ptr = NULL;
      hdr.last_index = m - 1;
      num_entries = hdr.last_index + 1;

      page *ret;

      // insert the key
      if (key < split_key) {
        insert_key(key, right, &num_entries, pred);
        ret = this;
      } else {
        sibling->insert_key(key, right, &sibling_cnt, pred);
        ret = sibling;
      }

      // Set a new root or insert the split key to the parent
      if (bt->root == (char *)this) { // only one node can update the root ptr
        page *new_root =
            new page((page *)this, split_key, sibling, hdr.level + 1);
        bt->setNewRoot((char *)new_root);

        if (with_lock) {
          hdr.mtx->unlock(); // Unlock the write lock
        }
      } else {
        if (with_lock) {
          hdr.mtx->unlock(); // Unlock the write lock
        }
        bt->btree_insert_internal(NULL, split_key, (char *)sibling,
                                  hdr.level + 1);
      }

      return ret;
    }
  }

  char *linear_search(entry_key_t key) {
    int i = 1;
    uint8_t previous_switch_counter;
    char *ret = NULL;
    char *t;
    entry_key_t k;

    if (hdr.leftmost_ptr == NULL) { // Search a leaf node
      do {
        previous_switch_counter = hdr.switch_counter;
        ret = NULL;

        // search from left ro right
        if (IS_FORWARD(previous_switch_counter)) {
          if ((k = records[0].key) == key) {
            if ((t = records[0].ptr) != NULL) {
              if (k == records[0].key) {
                ret = t;
                continue;
              }
            }
          }

          for (i = 1; records[i].ptr != NULL; ++i) {
            if ((k = records[i].key) == key) {
              if (records[i - 1].ptr != (t = records[i].ptr)) {
                if (k == records[i].key) {
                  ret = t;
                  break;
                }
              }
            }
          }
        } else { // search from right to left
          for (i = count() - 1; i > 0; --i) {
            if ((k = records[i].key) == key) {
              if (records[i - 1].ptr != (t = records[i].ptr) && t) {
                if (k == records[i].key) {
                  ret = t;
                  break;
                }
              }
            }
          }

          if (!ret) {
            if ((k = records[0].key) == key) {
              if (NULL != (t = records[0].ptr) && t) {
                if (k == records[0].key) {
                  ret = t;
                  continue;
                }
              }
            }
          }
        }
      } while (hdr.switch_counter != previous_switch_counter);

      if (ret) {
        return ret;
      }

      if ((t = (char *)hdr.sibling_ptr) && key >= ((page *)t)->records[0].key)
        return t;

      return NULL;
    } else { // internal node
      do {
        previous_switch_counter = hdr.switch_counter;
        ret = NULL;

        if (IS_FORWARD(previous_switch_counter)) {
          if (key < (k = records[0].key)) {
            if ((t = (char *)hdr.leftmost_ptr) != records[0].ptr) {
              ret = t;
              continue;
            }
          }

          for (i = 1; records[i].ptr != NULL; ++i) {
            if (key < (k = records[i].key)) {
              if ((t = records[i - 1].ptr) != records[i].ptr) {
                ret = t;
                break;
              }
            }
          }

          if (!ret) {
            ret = records[i - 1].ptr;
            continue;
          }
        } else { // search from right to left
          for (i = count() - 1; i >= 0; --i) {
            if (key >= (k = records[i].key)) {
              if (i == 0) {
                if ((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                  ret = t;
                  break;
                }
              } else {
                if (records[i - 1].ptr != (t = records[i].ptr)) {
                  ret = t;
                  break;
                }
              }
            }
          }
        }
      } while (hdr.switch_counter != previous_switch_counter);

      if ((t = (char *)hdr.sibling_ptr) != NULL) {
        if (key >= ((page *)t)->records[0].key)
          return t;
      }

      if (ret) {
        return ret;
      } else
        return (char *)hdr.leftmost_ptr;
    }

    return NULL;
  }

  char *linear_search_pred(entry_key_t key, char **pred, bool debug = false) {
    int i = 1;
    uint8_t previous_switch_counter;
    char *ret = NULL;
    char *t;
    entry_key_t k, k1;

    if (hdr.leftmost_ptr == NULL) { // Search a leaf node
      do {
        previous_switch_counter = hdr.switch_counter;
        ret = NULL;

        // search from left to right
        if (IS_FORWARD(previous_switch_counter)) {
          if (debug) {
            printf("search from left to right\n");
            printf("page:\n");
            printAll();
          }
          k = records[0].key;
          if (key < k) {
            if (hdr.pred_ptr != NULL) {
              *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
              if (debug)
                printf("line 752, *pred=%p\n", *pred);
            }
          }
          if (key > k) {
            *pred = records[0].ptr;
            if (debug)
              printf("line 757, *pred=%p\n", *pred);
          }

          if (k == key) {
            if (hdr.pred_ptr != NULL) {
              *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
              if (debug)
                printf("line 772, *pred=%p\n", *pred);
            }
            if ((t = records[0].ptr) != NULL) {
              if (k == records[0].key) {
                ret = t;
                continue;
              }
            }
          }

          for (i = 1; records[i].ptr != NULL; ++i) {
            k = records[i].key;
            k1 = records[i - 1].key;
            if (k < key) {
              *pred = records[i].ptr;
              if (debug)
                printf("line 775, *pred=%p\n", *pred);
            }
            if (k == key) {
              if (records[i - 1].ptr != (t = records[i].ptr)) {
                if (k == records[i].key) {
                  ret = t;
                  break;
                }
              }
            }
          }
        } else { // search from right to left
          if (debug) {
            printf("search from right to left\n");
            printf("page:\n");
            printAll();
          }
          bool once = true;

          for (i = count() - 1; i > 0; --i) {
            if (debug)
              printf("line 793, i=%d, records[i].key=%ld\n", i, records[i].key);
            k = records[i].key;
            k1 = records[i - 1].key;
            if (k1 < key && once) {
              *pred = records[i - 1].ptr;
              if (debug)
                printf("line 794, *pred=%p\n", *pred);
              once = false;
            }
            if (k == key) {
              if (records[i - 1].ptr != (t = records[i].ptr) && t) {
                if (k == records[i].key) {
                  ret = t;
                  break;
                }
              }
            }
          }

          if (!ret) {
            k = records[0].key;
            if (key < k) {
              if (hdr.pred_ptr != NULL) {
                *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
                if (debug)
                  printf("line 811, *pred=%p\n", *pred);
              }
            }
            if (key > k)
              *pred = records[0].ptr;
            if (k == key) {
              if (hdr.pred_ptr != NULL) {
                *pred = hdr.pred_ptr->records[hdr.pred_ptr->count() - 1].ptr;
                if (debug)
                  printf("line 844, *pred=%p\n", *pred);
              }
              if (NULL != (t = records[0].ptr) && t) {
                if (k == records[0].key) {
                  ret = t;
                  continue;
                }
              }
            }
          }
        }
      } while (hdr.switch_counter != previous_switch_counter);

      if (ret) {
        return ret;
      }

      if ((t = (char *)hdr.sibling_ptr) && key >= ((page *)t)->records[0].key)
        return t;

      return NULL;
    } else { // internal node
      do {
        previous_switch_counter = hdr.switch_counter;
        ret = NULL;

        if (IS_FORWARD(previous_switch_counter)) {
          if (key < (k = records[0].key)) {
            if ((t = (char *)hdr.leftmost_ptr) != records[0].ptr) {
              ret = t;
              continue;
            }
          }

          for (i = 1; records[i].ptr != NULL; ++i) {
            if (key < (k = records[i].key)) {
              if ((t = records[i - 1].ptr) != records[i].ptr) {
                ret = t;
                break;
              }
            }
          }

          if (!ret) {
            ret = records[i - 1].ptr;
            continue;
          }
        } else { // search from right to left
          for (i = count() - 1; i >= 0; --i) {
            if (key >= (k = records[i].key)) {
              if (i == 0) {
                if ((char *)hdr.leftmost_ptr != (t = records[i].ptr)) {
                  ret = t;
                  break;
                }
              } else {
                if (records[i - 1].ptr != (t = records[i].ptr)) {
                  ret = t;
                  break;
                }
              }
            }
          }
        }
      } while (hdr.switch_counter != previous_switch_counter);

      if ((t = (char *)hdr.sibling_ptr) != NULL) {
        if (key >= ((page *)t)->records[0].key)
          return t;
      }

      if (ret) {
        return ret;
      } else
        return (char *)hdr.leftmost_ptr;
    }

    return NULL;
  }
  // print a node
  void print() {
    if (hdr.leftmost_ptr == NULL)
      printf("[%d] leaf %p \n", this->hdr.level, this);
    else
      printf("[%d] internal %p \n", this->hdr.level, this);
    printf("last_index: %d\n", hdr.last_index);
    printf("switch_counter: %d\n", hdr.switch_counter);
    printf("search direction: ");
    if (IS_FORWARD(hdr.switch_counter))
      printf("->\n");
    else
      printf("<-\n");

    if (hdr.leftmost_ptr != NULL)
      printf("%p ", hdr.leftmost_ptr);

    for (int i = 0; records[i].ptr != NULL; ++i)
      printf("%ld,%p ", records[i].key, records[i].ptr);

    printf("\n%p ", hdr.sibling_ptr);

    printf("\n");
  }

  void printAll() {
    if (hdr.leftmost_ptr == NULL) {
      printf("printing leaf node: ");
      print();
    } else {
      printf("printing internal node: ");
      print();
      ((page *)hdr.leftmost_ptr)->printAll();
      for (int i = 0; records[i].ptr != NULL; ++i) {
        ((page *)records[i].ptr)->printAll();
      }
    }
  }
};
#ifdef USE_PMDK
inline int file_exists(const char *filename) {
  struct stat buffer;
  return stat(filename, &buffer);
}

inline void openPmemobjPool(std::string db_path, uint64_t pool_size) {
  printf("use pmdk!\n");
  std::string pool_path = db_path + "/pool";
  int sds_write_value = 0;
  pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
  if (file_exists(pool_path.c_str()) != 0) {
    printf("create new one.\n");
    if ((pop = pmemobj_create(pool_path.c_str(), POBJ_LAYOUT_NAME(btree),
                              pool_size, 0666)) == NULL) {
      perror("failed to create pool.\n");
      return;
    }
  } else {
    printf("open existing one.\n");
    if ((pop = pmemobj_open(pool_path.c_str(), POBJ_LAYOUT_NAME(btree))) ==
        NULL) {
      perror("failed to open pool.\n");
      return;
    }
  }
}
#endif
