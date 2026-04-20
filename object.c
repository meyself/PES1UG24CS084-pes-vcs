// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // TODO: Implement
    { const char *type_str = (type == OBJ_BLOB) ? "blob" : (type == OBJ_TREE ? "tree" : "commit");
    char header[64];
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1; // +1 for '\0'

    size_t full_len = header_len + len;
    void *full_data = malloc(full_len);
    if (!full_data) return -1;
    
    memcpy(full_data, header, header_len);
    memcpy((char*)full_data + header_len, data, len);

    compute_hash(full_data, full_len, id_out);

    if (object_exists(id_out)) {
        free(full_data);
        return 0;
    }

    char path[512];
    object_path(id_out, path, sizeof(path));

    char dir_path[512];
    snprintf(dir_path, sizeof(dir_path), "%s", path);
    char *last_slash = strrchr(dir_path, '/');
    if (last_slash) *last_slash = '\0';
    mkdir(dir_path, 0755);

    char tmp_path[512];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);

    int fd = open(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) { free(full_data); return -1; }
    if (write(fd, full_data, full_len) != (ssize_t)full_len) {
        close(fd); free(full_data); return -1;
    }
    fsync(fd);
    close(fd);

    rename(tmp_path, path);
    free(full_data);
    return 0;
}

    (void)type; (void)data; (void)len; (void)id_out;
    return -1;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // TODO: Implement
   { char path[512];
    object_path(id, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);

    void *file_data = malloc(file_size);
    if (!file_data || fread(file_data, 1, file_size, f) != (size_t)file_size) {
        if(file_data) free(file_data);
        fclose(f); return -1;
    }
    fclose(f);

    ObjectID computed_id;
    compute_hash(file_data, file_size, &computed_id);
    if (memcmp(computed_id.hash, id->hash, HASH_SIZE) != 0) {
        free(file_data); return -1;
    }

    char *null_byte = memchr(file_data, '\0', file_size);
    if (!null_byte) { free(file_data); return -1; }

    if (strncmp(file_data, "blob ", 5) == 0) *type_out = OBJ_BLOB;
    else if (strncmp(file_data, "tree ", 5) == 0) *type_out = OBJ_TREE;
    else if (strncmp(file_data, "commit ", 7) == 0) *type_out = OBJ_COMMIT;
    else { free(file_data); return -1; }

    size_t header_len = null_byte - (char *)file_data + 1;
    *len_out = file_size - header_len;
    *data_out = malloc(*len_out);
    if (!*data_out) { free(file_data); return -1; }
    
    memcpy(*data_out, null_byte + 1, *len_out);
    free(file_data);
    return 0;
}
    (void)id; (void)type_out; (void)data_out; (void)len_out;
    return -1;
}
