// 'mskip' implements a memory-optimized skip list using an arena allocator, offering O(log n) average time complexity for operations.
package mskip

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"gosuda.org/sseuda"
	"gosuda.org/sseuda/internal/oldsepia/marena"
	"gosuda.org/sseuda/internal/oldsepia/splitmix64"
)

const (
	MSKIP_MAX_LEVEL = 24 // Defines the maximum height of the skip list. Higher values improve search performance in large lists but increase memory consumption per node.
)

// mskipNode's memory layout within the arena:
//   keyPtr:    uint64      // Offset to the key's bytes in the arena.
//   valuePtr:  uint64      // Offset to the value's bytes in the arena.
//   level:     int32       // The node's current height, from 1 to MSKIP_MAX_LEVEL.
//   reserved:  4 bytes     // Padding for alignment.
//   nexts:     uint32[level] // Array of offsets to next nodes, one for each level.

// mskipNode represents an individual node in the skip list.
// The actual memory footprint of a node dynamically adjusts based on its `level`.
type mskipNode struct {
	keyPtr   uint64                  // Stores the arena offset for the node's key.
	valuePtr uint64                  // Stores the arena offset for the node's value.
	level    int32                   // Indicates the height or level of the node (1 to MSKIP_MAX_LEVEL).
	nexts    [MSKIP_MAX_LEVEL]uint32 // An array of arena offsets pointing to the next nodes at each level.
}

// Constants defining the memory offsets and sizes of `mskipNode` fields.
// These are used for compile-time assertions and dynamic size calculations, ensuring memory layout consistency.
const (
	nodeKeyPtrOffset   = unsafe.Offsetof(mskipNode{}.keyPtr)   // Byte offset of the `keyPtr` field.
	nodeValuePtrOffset = unsafe.Offsetof(mskipNode{}.valuePtr) // Byte offset of the `valuePtr` field.
	nodeLevelOffset    = unsafe.Offsetof(mskipNode{}.level)    // Byte offset of the `level` field.
	nodeNextsOffset    = unsafe.Offsetof(mskipNode{}.nexts)    // Byte offset of the `nexts` array.
	nodeMaxSize        = unsafe.Sizeof(mskipNode{})            // The maximum size a node can occupy assumes MSKIP_MAX_LEVEL is used.
)

// Compile-time checks to protect against unintended changes in `mskipNode`'s memory layout.
var _ = [1]struct{}{}[nodeKeyPtrOffset-0]   // `keyPtr` must be at offset 0.
var _ = [1]struct{}{}[nodeValuePtrOffset-8] // `valuePtr` must be at offset 8.
var _ = [1]struct{}{}[nodeLevelOffset-16]   // `level` must be at offset 16.
var _ = [1]struct{}{}[nodeNextsOffset-20]   // `nexts` must be at offset 20.
var _ = [1]struct{}{}[nodeMaxSize-120]      // Total size must be 120 bytes.

// sizeNode computes the precise memory required for a node given its `nodeLevel`.
// This function optimizes memory usage by allocating only for the explicit `nodeLevel`,
// rather than the maximum possible level.
func sizeNode(nodeLevel int32) uintptr {
	return uintptr(nodeMaxSize - MSKIP_MAX_LEVEL*4 + uintptr(nodeLevel)*4)
}

// SkipList is a memory-optimized skip list implementation that uses an arena allocator
// for efficient memory management of nodes, keys, and values, which are stored as byte slices.
type SkipList struct {
	arena    *marena.Arena               // Manages all memory allocations for the skip list.
	seed     uint64                      // Seed for the random level generation, ensuring probabilistic balance.
	head     uint32                      // Arena offset pointing to the head node of the skip list.
	compare  func(key1, key2 []byte) int // Function to compare keys: -1 (key1 < key2), 0 (key1 == key2), or 1 (key1 > key2).
	refCount int64                       // Atomically managed reference count for the skip list instance.
}

// IncRef atomically increments the skip list's reference count.
// It returns the updated reference count.
func (g *SkipList) IncRef() int64 {
	return atomic.AddInt64(&g.refCount, 1)
}

// DecRef atomically decrements the skip list's reference count.
// It returns the updated reference count.
func (g *SkipList) DecRef() int64 {
	return atomic.AddInt64(&g.refCount, -1)
}

// RefCount returns the current, atomically loaded reference count of the skip list.
func (g *SkipList) RefCount() int64 {
	return atomic.LoadInt64(&g.refCount)
}

// NewSkipList initializes and returns a new SkipList instance.
// It requires an `arena` for memory management, a `compare` function for key ordering, and a `seed` for level randomization.
// Returns an error if the initial allocation for the head node fails.
func NewSkipList(arena *marena.Arena, compare func(key1, key2 []byte) int, seed uint64) (*SkipList, error) {
	g := &SkipList{
		arena:    arena,
		seed:     seed,
		head:     0,
		compare:  compare,
		refCount: 1,
	}

	// Allocate and set up the head node to its maximum possible level.
	headSize := sizeNode(MSKIP_MAX_LEVEL)
	headPtr := arena.Allocate(int(headSize))
	if headPtr == marena.ARENA_INVALID_ADDRESS {
		return nil, marena.ErrAllocationFailed
	}

	// Initialize the head node's fields.
	head := g.getNode(marena.Offset(headPtr))
	head.level = MSKIP_MAX_LEVEL
	head.keyPtr = marena.ARENA_INVALID_ADDRESS
	head.valuePtr = marena.ARENA_INVALID_ADDRESS
	for i := int32(0); i < MSKIP_MAX_LEVEL; i++ {
		head.nexts[i] = marena.ARENA_INVALID_ADDRESS
	}
	g.head = marena.Offset(headPtr)

	return g, nil
}

// randLevel determines a random level for a new node using a geometric distribution.
// This approach maintains the probabilistic balance of the skip list, where the likelihood
// of a node having level `k` is (1/2)^(k-1).
func (g *SkipList) randLevel() int32 {
	level := int32(1)
	for level < MSKIP_MAX_LEVEL && splitmix64.Splitmix64(&g.seed)%2 == 0 {
		level++
	}
	return level
}

// getNode transforms an arena offset (`ptr`) into an `mskipNode` pointer.
// This operation is `unsafe` and directly interacts with the arena's memory.
func (g *SkipList) getNode(ptr uint32) *mskipNode {
	return (*mskipNode)(unsafe.Pointer(g.arena.Index(ptr)))
}

// seeklt locates the node with the largest key strictly less than `key`.
// It traverses the skip list from the highest level down, using the provided `log` to record
// the path taken at each level. This path is crucial for efficient insertion.
// If `log` is `nil`, a dummy log is used, and no path is recorded.
// Returns the arena offset of the located node.
func (g *SkipList) seeklt(key []byte, log *[MSKIP_MAX_LEVEL]uint32) uint32 {
	ptr := g.head

	// Initialize log if not provided.
	var dummyLog [MSKIP_MAX_LEVEL]uint32
	if log == nil {
		log = &dummyLog
	}

	// Traverse from the highest level down to find the predecessor node.
	for i := MSKIP_MAX_LEVEL - 1; i >= 0; i-- {
		// Invariant: `ptr` becomes the last node with key < `key` at this level.
		for {
			next := g.getNode(ptr).nexts[i]
			if next == marena.ARENA_INVALID_ADDRESS || g.compare(key, g.arena.View(g.getNode(next).keyPtr)) <= 0 {
				break // Either end of list or found a node >= `key`.
			}
			ptr = next
		}
		log[i] = ptr // Record the path at level `i`.
	}

	return ptr
}

// insertNext inserts a new key-value pair based on the insertion `log` from `seeklt`.
// If `key` already exists, its `value` is updated. If `value` is `nil`, the existing entry is marked as deleted.
//
// Parameters:
//   - log: An array containing the skip list path used to find the insertion point.
//   - key: The key to be inserted or updated.
//   - value: The value to associate with the key; `nil` marks the key for deletion.
//
// Returns: The arena offset of the new or updated node, or `marena.ARENA_INVALID_ADDRESS` if allocation fails.
func (g *SkipList) insertNext(log *[MSKIP_MAX_LEVEL]uint32, key []byte, value []byte) uint32 {
	next := g.getNode(log[0]).nexts[0]
	// If key exists, update its value.
	if next != marena.ARENA_INVALID_ADDRESS && g.compare(key, g.arena.View(g.getNode(next).keyPtr)) == 0 {
		if value == nil {
			g.getNode(next).valuePtr = marena.ARENA_INVALID_ADDRESS // Mark as deleted by setting valuePtr to invalid.
		} else {
			newValueAddr := g.arena.Allocate(len(value))
			if newValueAddr == marena.ARENA_INVALID_ADDRESS {
				return marena.ARENA_INVALID_ADDRESS // Allocation failed for new value.
			}
			copy(g.arena.View(newValueAddr), value)
			g.getNode(next).valuePtr = newValueAddr
		}
		return next
	}

	// Determine new node's level and calculate sizes for allocation.
	level := g.randLevel()
	var newNodeSize uint64 = uint64(sizeNode(level))
	var newKeySize uint64 = uint64(len(key))

	// Allocate memory for the new node and its key.
	if !g.arena.AllocateMultiple(&newNodeSize, &newKeySize) {
		return marena.ARENA_INVALID_ADDRESS // Failed to allocate for node or key.
	}

	// Initialize the new node.
	node := g.getNode(marena.Offset(newNodeSize))
	node.level = level
	node.keyPtr = newKeySize
	copy(g.arena.View(node.keyPtr), key)

	// Allocate and copy the value if not nil.
	if value == nil {
		node.valuePtr = marena.ARENA_INVALID_ADDRESS // Mark as deleted.
	} else {
		newValueAddr := g.arena.Allocate(len(value))
		if newValueAddr == marena.ARENA_INVALID_ADDRESS {
			return marena.ARENA_INVALID_ADDRESS // Failed to allocate for value.
		}
		copy(g.arena.View(newValueAddr), value)
		node.valuePtr = newValueAddr
	}

	// Update `nexts` pointers to link the new node into the skip list at appropriate levels.
	for i := int32(0); i < level; i++ {
		node.nexts[i] = g.getNode(log[i]).nexts[i]              // New node points to what the predecessor at this level was pointing to.
		g.getNode(log[i]).nexts[i] = marena.Offset(newNodeSize) // Predecessor now points to the new node.
	}

	return marena.Offset(newNodeSize)
}

// Insert adds or updates a key-value pair in the skip list.
// Keys and values are managed as byte slices within the arena allocator.
// Returns `true` on successful insertion/update, `false` if memory allocation fails.
func (g *SkipList) Insert(key []byte, value []byte) bool {
	var log [MSKIP_MAX_LEVEL]uint32
	g.seeklt(key, &log)
	return g.insertNext(&log, key, value) != marena.ARENA_INVALID_ADDRESS
}

// iteratorPool reuses SkipListIterator instances to reduce memory allocations.
var iteratorPool = sync.Pool{
	New: func() interface{} {
		return &SkipListIterator{}
	},
}

// SkipListIterator enables bidirectional traversal of skip list entries.
// It tracks its current position and supports forward, backward, and seek operations.
type SkipListIterator struct {
	skl     *SkipList // Reference to the skip list being iterated.
	current uint32    // Arena offset of the current node.
}

var _ sseuda.Iterator = (*SkipListIterator)(nil)

// Iterator returns a new SkipListIterator for traversing the skip list.
// The iterator is initially invalid and must be positioned using methods like `First()`, `SeekLT()`, or `SeekLE()`.
// It's crucial to call `Close()` on the iterator when it's no longer needed to release resources.
func (g *SkipList) Iterator() *SkipListIterator {
	g.IncRef() // Increment the skip list's reference count.
	iter := iteratorPool.Get().(*SkipListIterator)
	iter.skl = g
	iter.current = marena.ARENA_INVALID_ADDRESS // Initialize to an invalid position.
	return iter
}

// First attempts to position the iterator at the smallest key in the skip list.
// Returns `true` if successful (i.e., the skip list is not empty), otherwise `false`.
func (g *SkipListIterator) First() bool {
	g.current = g.skl.head // Start from the head node.
	g.Next()               // Advance to the first actual data node.
	return g.Valid()
}

// seeklt positions the iterator to the largest key strictly less than `key`.
// The iterator becomes invalid if no such key exists.
func (g *SkipListIterator) seeklt(key []byte) {
	var log [MSKIP_MAX_LEVEL]uint32 // A log is used internally but not exposed to the iterator user.
	g.current = g.skl.seeklt(key, &log)
}

// seekle positions the iterator at the largest key less than or equal to `key`.
// If `key` is found, the iterator points to that exact key.
// If no such key exists, the iterator becomes invalid.
func (g *SkipListIterator) seekle(key []byte) {
	var log [MSKIP_MAX_LEVEL]uint32
	// Find the largest key strictly less than `key`.
	prevNodePtr := g.skl.seeklt(key, &log)

	// If `prevNodePtr` is the head, it means all keys in the skip list are greater than or equal to `key`.
	// Check if the very first node matches `key`.
	if prevNodePtr == g.skl.head {
		firstNodePtr := g.skl.getNode(g.skl.head).nexts[0]
		if firstNodePtr != marena.ARENA_INVALID_ADDRESS && g.skl.compare(key, g.skl.arena.View(g.skl.getNode(firstNodePtr).keyPtr)) == 0 {
			g.current = firstNodePtr // Found an exact match at the beginning.
		} else {
			g.current = marena.ARENA_INVALID_ADDRESS // No key <= `key` found.
		}
		return
	}

	// Otherwise, `prevNodePtr` points to a node whose key is < `key`.
	// Now, check if the node immediately following `prevNodePtr` is an exact match for `key`.
	g.current = prevNodePtr // Initialize iterator to the node found by `seeklt`.

	nextNodePtr := g.skl.getNode(g.current).nexts[0]
	if nextNodePtr != marena.ARENA_INVALID_ADDRESS && g.skl.compare(key, g.skl.arena.View(g.skl.getNode(nextNodePtr).keyPtr)) == 0 {
		g.current = nextNodePtr // Move to the exact match.
	}
	// If `nextNodePtr` isn't an exact match, `g.current` remains `prevNodePtr`, which is the largest key <= `key`.
	// This is the correct behavior for `SeekLE`.
}

// Valid reports whether the iterator is currently positioned at a valid key-value pair.
// Returns `true` if valid, `false` otherwise (e.g., past the end or uninitialized).
func (g *SkipListIterator) Valid() bool {
	return g.current != marena.ARENA_INVALID_ADDRESS
}

// Next advances the iterator to the next valid key-value pair in the skip list.
// If the iterator is already invalid or at the end of the list, it remains invalid.
// `Next` automatically skips "tombstone" (soft-deleted) entries.
// Returns `true` if the iterator is now valid, `false` otherwise.
func (g *SkipListIterator) Next() bool {
	if !g.Valid() {
		return false
	}

	for {
		node := g.skl.getNode(g.current)
		g.current = node.nexts[0] // Move to the next node at level 0.
		if !g.Valid() || g.Value() != nil {
			break // Stop if invalid or found a non-nil (valid) value.
		}
	}
	return g.Valid()
}

// Prev moves the iterator to the previous valid key-value pair in the skip list.
// If the iterator is invalid or at the first key, it becomes invalid.
// Returns `true` if the iterator is now valid, `false` otherwise.
func (g *SkipListIterator) Prev() bool {
	if !g.Valid() {
		return false
	}

	// Use seeklt to find the node strictly before the current node's key.
	currentKeyBytes := g.skl.arena.View(g.skl.getNode(g.current).keyPtr)
	prevNodePtr := g.skl.seeklt(currentKeyBytes, nil)

	if prevNodePtr == g.skl.head {
		g.current = marena.ARENA_INVALID_ADDRESS // No previous element found (current was the first or only).
		return false
	}
	g.current = prevNodePtr
	// Note: `seeklt` might return a tombstone entry if `prevNodePtr`'s value is nil.
	// The `Next()` method handles skipping tombstones. `Prev()` does not automatically.
	// This might need further refinement depending on desired Prev() behavior with tombstones.
	return true
}

// Key returns the key of the current entry.
// Returns `nil` if the iterator is not valid.
func (g *SkipListIterator) Key() []byte {
	if !g.Valid() {
		return nil
	}
	node := g.skl.getNode(g.current)
	return g.skl.arena.View(node.keyPtr)
}

// Value returns the value of the current entry.
// Returns `nil` if the iterator is not valid, or if the entry is a tombstone.
func (g *SkipListIterator) Value() []byte {
	if !g.Valid() {
		return nil
	}
	node := g.skl.getNode(g.current)
	if node.valuePtr == marena.ARENA_INVALID_ADDRESS {
		return nil // Value indicates a tombstone or no value.
	}
	return g.skl.arena.View(node.valuePtr)
}

// Seek positions the iterator to the first key greater than or equal to `key`.
// If no such key exists, the iterator will be invalid.
func (g *SkipListIterator) Seek(key []byte) bool {
	g.seekle(key) // First, find the largest key less than or equal to `key`.
	// If `seekle` positioned the iterator at a key strictly less than `key`,
	// advance to the next element until the condition `currentKey >= key` is met.
	if g.Valid() && g.skl.compare(g.Key(), key) < 0 {
		g.Next()
	}
	return g.Valid()
}

// Close releases the iterator's underlying resources and returns it to the pool.
// After calling `Close`, the iterator becomes invalid and should not be used further.
// This method also decrements the reference count of the associated skip list.
func (g *SkipListIterator) Close() error {
	if g == nil {
		return nil
	}
	g.skl.DecRef() // Decrement the skip list's reference count.
	g.skl = nil
	g.current = marena.ARENA_INVALID_ADDRESS
	iteratorPool.Put(g) // Return the iterator to the pool for reuse.
	return nil
}
