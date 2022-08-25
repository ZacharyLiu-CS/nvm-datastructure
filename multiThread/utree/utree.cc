#include "utree.h"

/*
 * class btree
 */
btree::btree(std::string db_path, uint64_t pool_size) {
#ifdef USE_PMDK
  openPmemobjPool(db_path, pool_size);
#else
  printf("without pmdk!\n");
#endif
  root = (char *)new page();
  list_head = (list_node_t *)alloc(sizeof(list_node_t));
  printf("list_head=%p\n", list_head);
  list_head->next = NULL;
  height = 1;
}

btree::~btree() {
#ifdef USE_PMDK
  pmemobj_close(pop);
#endif
}

void btree::setNewRoot(char *new_root) {
  this->root = (char *)new_root;
  ++height;
}

char *btree::btree_search_pred(entry_key_t key, bool *f, char **prev,
                               bool debug ) {
  page *p = (page *)root;

  while (p->hdr.leftmost_ptr != NULL) {
    p = (page *)p->linear_search(key);
  }

  page *t;
  while ((t = (page *)p->linear_search_pred(key, prev, debug)) ==
         p->hdr.sibling_ptr) {
    p = t;
    if (!p) {
      break;
    }
  }

  if (!t) {
    // printf("NOT FOUND %lu, t = %p\n", key, t);
    *f = false;
    return NULL;
  }

  *f = true;
  return (char *)t;
}

char *btree::search(entry_key_t key) {
  bool f = false;
  char *prev;
  char *ptr = btree_search_pred(key, &f, &prev);
  if (f) {
    list_node_t *n = (list_node_t *)ptr;
    if (n->ptr != 0)
      return (char *)n->ptr;
  } else {
    ; // printf("not found.\n");
  }
  return NULL;
}

// insert the key in the leaf node
void btree::btree_insert_pred(entry_key_t key, char *right, char **pred,
                              bool *update) { // need to be string
  page *p = (page *)root;

  while (p->hdr.leftmost_ptr != NULL) {
    p = (page *)p->linear_search(key);
  }
  *pred = NULL;
  if (!p->store(this, NULL, key, right, true, true, pred)) { // store
    // The key already exist.
    *update = true;
  } else {
    // Insert a new key.
    *update = false;
  }
}

void btree::insert(entry_key_t key, char *right) {
  list_node_t *n = (list_node_t *)alloc(sizeof(list_node_t));
  // printf("n=%p\n", n);
  n->next = NULL;
  n->key = key;
  n->ptr = (uint64_t)right;
  n->isUpdate = false;
  n->isDelete = false;
  list_node_t *prev = NULL;
  bool update;
  bool rt = false;
  btree_insert_pred(key, (char *)n, (char **)&prev, &update);
  if (update && prev != NULL) {
    // Overwrite.
    prev->ptr = (uint64_t)right;
    // flush.
    clflush((char *)prev, sizeof(list_node_t));
  } else {
    int retry_number = 0, w = 0;
  retry:
    retry_number += 1;
    if (retry_number > 10 && w == 3) {
      return;
    }
    if (rt) {
      // we need to re-search the key!
      bool f;
      btree_search_pred(key, &f, (char **)&prev);
      if (!f) {
        return;
        printf("error!!!!\n");
        exit(0);
      }
    }
    rt = true;
    // Insert a new key.
    if (list_head->next != NULL) {

      if (prev == NULL) {
        // Insert a smallest one.
        prev = list_head;
      }
      if (prev->isUpdate) {
        w = 1;
        goto retry;
      }

      // check the order and CAS.
      list_node_t *next = prev->next;
      n->next = next;
      clflush((char *)n, sizeof(list_node_t));
      if (prev->key < key && (next == NULL || next->key > key)) {
        if (!__sync_bool_compare_and_swap(&(prev->next), next, n)) {
          w = 2;
          goto retry;
        }

        clflush((char *)prev, sizeof(list_node_t));
      } else {
        // View changed, retry.
        w = 3;
        goto retry;
      }
    } else {
      // This is the first insert!
      if (!__sync_bool_compare_and_swap(&(list_head->next), NULL, n))
        goto retry;
    }
  }
}

void btree::remove(entry_key_t key) {
  bool f, debug = false;
  list_node_t *cur = NULL, *prev = NULL;
retry:
  cur = (list_node_t *)btree_search_pred(key, &f, (char **)&prev, debug);
  if (!f) {
    printf("not found.\n");
    return;
  }
  if (prev == NULL) {
    prev = list_head;
  }
  if (prev->next != cur) {
    if (debug) {
      printf("prev list node:\n");
      prev->printAll();
      printf("current list node:\n");
      cur->printAll();
    }
    exit(1);
    goto retry;
  } else {
    // Delete it.
    if (!__sync_bool_compare_and_swap(&(prev->next), cur, cur->next))
      goto retry;
    clflush((char *)prev, sizeof(list_node_t));
    btree_delete(key);
  }
}

// store the key into the node at the given level
void btree::btree_insert_internal(char *left, entry_key_t key, char *right,
                                  uint32_t level) {
  if (level > ((page *)root)->hdr.level)
    return;

  page *p = (page *)this->root;

  while (p->hdr.level > level)
    p = (page *)p->linear_search(key);

  if (!p->store(this, NULL, key, right, true, true)) {
    btree_insert_internal(left, key, right, level);
  }
}

void btree::btree_delete(entry_key_t key) {
  page *p = (page *)root;

  while (p->hdr.leftmost_ptr != NULL) {
    p = (page *)p->linear_search(key);
  }

  page *t;
  while ((t = (page *)p->linear_search(key)) == p->hdr.sibling_ptr) {
    p = t;
    if (!p)
      break;
  }

  if (p) {
    if (!p->remove(this, key)) {
      btree_delete(key);
    }
  } else {
    printf("not found the key to delete %lu\n", key);
  }
}

void btree::printAll() {
  pthread_mutex_lock(&print_mtx);
  int total_keys = 0;
  page *leftmost = (page *)root;
  printf("root: %p\n", root);
  do {
    page *sibling = leftmost;
    while (sibling) {
      if (sibling->hdr.level == 0) {
        total_keys += sibling->hdr.last_index + 1;
      }
      sibling->print();
      sibling = sibling->hdr.sibling_ptr;
    }
    printf("-----------------------------------------\n");
    leftmost = leftmost->hdr.leftmost_ptr;
  } while (leftmost);

  printf("total number of keys: %d\n", total_keys);
  pthread_mutex_unlock(&print_mtx);
}
