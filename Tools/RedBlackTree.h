#ifndef REDBLACKTREE_H
#define REDBLACKTREE_H

#include <stddef.h>

/*

  Based on Julienne Walker's <http://eternallyconfuzzled.com/> RedBlackTree implementation
  Modified by Mirek Rusin <http://github.com/mirek/RedBlackTree>

  https://github.com/mirek/rb_tree/
  http://www.eternallyconfuzzled.com/tuts/datastructures/jsw_tut_rbtree.aspx
  https://github.com/Nu3001/external_chromium_org/blob/master/third_party/bintrees/bintrees/rbtree.py

  Re-written by Artem Prilutskiy (cyanide.burnout@gmail.com)

*/

#ifdef __cplusplus
extern "C"
{
#endif

#define REDBLACK_MAXIMUM_HEIGHT 64  // Tallest allowable tree to iterate

#define REDBLACK_COLOR_BLACK  0
#define REDBLACK_COLOR_RED    1

#define REDBLACK_LINK_LEFT    0
#define REDBLACK_LINK_RIGHT   1


struct RedBlackNode;
struct RedBlackTree;

typedef int (*CompareRedBlackNode) (struct RedBlackTree* tree, struct RedBlackNode* node, const void* key1, const void* key2);
typedef void (*ReleaseRedBlackNode) (struct RedBlackTree* tree, void* value);

struct RedBlackNode
{
  int color;                     // Color red (1), black (0)
  struct RedBlackNode* link[2];  // Link left [0] and right [1]
  const void* key;               // User provided, used indirectly via CompareRedBlackNode
  void* value;                   // User provided, used indirectly via CompareRedBlackNode
};

struct RedBlackTree
{
  struct RedBlackNode* root;
  CompareRedBlackNode compare;
  ReleaseRedBlackNode release;
  size_t size;
  void* data;  // User provided, not used by RedBlackTree.
};

struct RedBlackIterator
{
  struct RedBlackTree* tree;
  struct RedBlackNode* node;                           // Current node
  struct RedBlackNode* path[REDBLACK_MAXIMUM_HEIGHT];  // Traversal path
  size_t top;                                          // Top of stack
  void* data;                                          // User provided, not used by RedBlackIterator
};


struct RedBlackTree* CreateRedBlackTree(CompareRedBlackNode function1, ReleaseRedBlackNode function2, void* data);
void ReleaseRedBlackTree(struct RedBlackTree* tree);

void* GetFromRedBlackTree(struct RedBlackTree* tree, const void* key);

int PutIntoRedBlackTree(struct RedBlackTree* tree, const void* key, void* value);
int RemoveFromRedBlackTree(struct RedBlackTree* tree, const void* key, void* value);

void* StartRedBlackIterator(struct RedBlackIterator* iterator, struct RedBlackTree* tree, int direction);
void* MoveRedBlackIterator(struct RedBlackIterator* iterator, int direction);

void* StartRedBlackIteratorFromValue(struct RedBlackIterator* iterator, struct RedBlackTree* tree, const void* key, int direction);

#define MoveFirstRedBlackValue(iterator, tree)  StartRedBlackIterator(iterator, tree, REDBLACK_LINK_LEFT)
#define MoveLastRedBlackValue(iterator, tree)   StartRedBlackIterator(iterator, tree, REDBLACK_LINK_RIGHT)
#define MoveNextRedBlackValue(iterator)         MoveRedBlackIterator(iterator, REDBLACK_LINK_RIGHT)
#define MovePreviousRedBlackValue(iterator)     MoveRedBlackIterator(iterator, REDBLACK_LINK_LEFT)

int CompareStringKeys(struct RedBlackTree* tree, struct RedBlackNode* node, const void* key1, const void* key2);
int CompareIntegerKeys(struct RedBlackTree* tree, struct RedBlackNode* node, const void* key1, const void* key2);
int CompareLongIntegerKeys(struct RedBlackTree* tree, struct RedBlackNode* node, const void* key1, const void* key2);

#ifdef __cplusplus
}

#include <cstddef>
#include <iterator>
#include <functional>

template<typename Type> class RedBlackTreeIterator
{
  public:

    using pointer           = Type*;
    using reference         = Type&;
    using value_type        = Type;
    using difference_type   = std::ptrdiff_t;
    using iterator_category = std::input_iterator_tag;

    typedef std::function<bool (const Type*)> Filter;

    RedBlackTreeIterator()                                                                                                                                  noexcept                                            { iterator.node = nullptr;                                                     };
    RedBlackTreeIterator(struct RedBlackTree* tree, int initial = REDBLACK_LINK_LEFT, int further = REDBLACK_LINK_RIGHT)                                    noexcept : direction(further)                       { StartRedBlackIterator(&iterator, tree, initial);                             };
    RedBlackTreeIterator(struct RedBlackTree* tree, const void* key, int initial = REDBLACK_LINK_RIGHT, int further = REDBLACK_LINK_RIGHT)                  noexcept : direction(further)                       { StartRedBlackIteratorFromValue(&iterator, tree, key, initial);               };
    RedBlackTreeIterator(struct RedBlackTree* tree, Filter function, int initial = REDBLACK_LINK_LEFT,  int further = REDBLACK_LINK_RIGHT)                  : direction(further), function(std::move(function)) { StartRedBlackIterator(&iterator, tree, initial);                validate();  };
    RedBlackTreeIterator(struct RedBlackTree* tree, const void* key, Filter function, int initial = REDBLACK_LINK_RIGHT, int further = REDBLACK_LINK_RIGHT) : direction(further), function(std::move(function)) { StartRedBlackIteratorFromValue(&iterator, tree, key, initial);  validate();  };

    RedBlackTreeIterator& operator++()            { move();                                         return *this;   };
    RedBlackTreeIterator  operator++(int number)  { RedBlackTreeIterator current = *this;  move();  return current; };

    friend bool operator==(const RedBlackTreeIterator& value1, const RedBlackTreeIterator& value2)  noexcept  { return value1.iterator.node == value2.iterator.node; };
    friend bool operator!=(const RedBlackTreeIterator& value1, const RedBlackTreeIterator& value2)  noexcept  { return value1.iterator.node != value2.iterator.node; };

    RedBlackTreeIterator& operator*()  noexcept  { return *this;                  };
    RedBlackTreeIterator& begin()      noexcept  { return *this;                  };
    RedBlackTreeIterator  end()                  { return RedBlackTreeIterator(); };

    Type* operator->()                 noexcept  { return static_cast<Type*>(iterator.node->value); };
    Type* get()                        noexcept  { return static_cast<Type*>(iterator.node->value); };

    friend bool operator==(const Type* value1, const RedBlackTreeIterator& value2)  noexcept  { return value1 == static_cast<Type*>(value2.iterator.node->value); };
    friend bool operator!=(const Type* value1, const RedBlackTreeIterator& value2)  noexcept  { return value1 != static_cast<Type*>(value2.iterator.node->value); };
    friend bool operator==(const RedBlackTreeIterator& value1, const Type* value2)  noexcept  { return static_cast<Type*>(value1.iterator.node->value) == value2; };
    friend bool operator!=(const RedBlackTreeIterator& value1, const Type* value2)  noexcept  { return static_cast<Type*>(value1.iterator.node->value) != value2; };

  private:

    struct RedBlackIterator iterator;
    Filter function;
    int direction;

    void validate()
    {
      if (iterator.node &&
          function      &&
          !function(static_cast<Type*>(iterator.node->value)))
      {
        // Reset iterator to make it equal to end()
        iterator.node = nullptr;
      }
    };

    void move()
    {
      if (iterator.node)
      {
        MoveRedBlackIterator(&iterator, direction);
        validate();
      }
    };
};

#endif

#endif
