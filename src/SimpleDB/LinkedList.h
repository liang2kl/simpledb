#ifndef _SIMPLEDB_LINKED_LIST_H
#define _SIMPLEDB_LINKED_LIST_H

template <typename T>
class LinkedList {
public:
    struct Node {
        T *data;
        Node *next;
    };

    Node *insert(T *data) {
        // TODO
    }

    T *removeLRU() {
        // TODO: Implement LRU.
    }

    T *remove(Node *node) {}

    int size();

private:
    Node *head;
    Node *tail;
    int _size;
};

#endif