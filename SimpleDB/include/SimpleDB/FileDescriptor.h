#ifndef _SIMPLEDB_FILE_DESCRIPTOR
#define _SIMPLEDB_FILE_DESCRIPTOR

// The type for an external, transparent file descriptor. It's actually an
// integer, but we wrap it for type checking.
struct FileDescriptor {
    operator int() { return value; }
    explicit FileDescriptor(const int value) : value(value) {}

    int value;
};

bool operator==(const FileDescriptor &lhs, const FileDescriptor &rhs) {
    return lhs.value == rhs.value;
}

#endif