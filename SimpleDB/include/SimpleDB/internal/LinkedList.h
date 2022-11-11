#ifndef _SIMPLEDB_LINKED_LIST_H
#define _SIMPLEDB_LINKED_LIST_H

namespace SimpleDB {
namespace Internal {

template <typename T>
class LinkedList {
public:
    LinkedList() {
        head = new Node();
        tail = head;
        head->next = nullptr;
        head->prev = nullptr;
    }

    struct Node {
        T *data;
        Node *next;
        Node *prev;
    };

    /**
     * +-------+    +-------------+
     * | guard | -> | actual tail | -> ...
     * +-------+    +-------------+
     */
    Node *insertHead(T *data) {
        Node *node = new Node();
        node->data = data;
        head->next = node;
        node->next = nullptr;
        node->prev = head;
        head = node;
        _size++;
        return node;
    }

    T *removeTail() {
        if (head == tail) {
            return nullptr;
        }
        return remove(tail->next);
    }

    T *last() {
        if (head == tail) {
            return nullptr;
        }
        return tail->next->data;
    }

    T *remove(Node *node) {
        node->prev->next = node->next;
        if (node != head) {
            node->next->prev = node->prev;
        } else {
            head = node->prev;
        }

        T *data = node->data;
        delete node;
        _size--;

        return data;
    }

    inline int size() { return _size; };

#if !TESTING
private:
#endif
    Node *head;
    Node *tail;
    int _size = 0;
};

}  // namespace Internal
}  // namespace SimpleDB
#endif