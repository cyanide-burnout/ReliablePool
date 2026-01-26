#include "RedBlackTree.h"

#include <malloc.h>
#include <string.h>
#include <stdint.h>

#ifdef __OPTIMIZE__
#define INLINE  inline
#else
#define INLINE  static
#endif

#define TRUE   1
#define FALSE  0

// RedBlackNode

INLINE int IsRed(struct RedBlackNode* node)
{
  return
    (node != NULL) &&
    (node->color == REDBLACK_COLOR_RED);
}

static struct RedBlackNode* RotateRedBlackNodeOnce(struct RedBlackNode* node, int direction)
{
  struct RedBlackNode* save = node->link[!direction];
  node->link[!direction] = save->link[direction];
  save->link[ direction] = node;
  node->color = REDBLACK_COLOR_RED;
  save->color = REDBLACK_COLOR_BLACK;
  return save;
}

static struct RedBlackNode* RotateRedBlackNodeTwice(struct RedBlackNode* node, int direction)
{
  node->link[!direction] = RotateRedBlackNodeOnce(node->link[!direction], !direction);
  return                   RotateRedBlackNodeOnce(node,                    direction);
}

// RedBlackTree

struct RedBlackTree* CreateRedBlackTree(CompareRedBlackNode function1, ReleaseRedBlackNode function2, void* data)
{
   struct RedBlackTree* tree = (struct RedBlackTree*)calloc(1, sizeof(struct RedBlackTree));
   if (tree != NULL)
   {
     tree->compare = function1;
     tree->release = function2;
     tree->data = data;
   }
   return tree;
}

void ReleaseRedBlackTree(struct RedBlackTree* tree)
{
  if (tree != NULL)
  {
    struct RedBlackNode* node = tree->root;
    struct RedBlackNode* save = NULL;

    // Rotate away the left links so that
    // we can treat this like the destruction
    // of a linked list
    while (node != NULL)
    {
      if (node->link[REDBLACK_LINK_LEFT] == NULL)
      {
        // No left links, just kill the node and move on
        save = node->link[REDBLACK_LINK_RIGHT];
        if (tree->release != NULL)
        {
          // Call release function before releasing node
          tree->release(tree, node->value);
        }
        free(node);
      }
      else
      {
        // Rotate away the left link and check again
        save = node->link[REDBLACK_LINK_LEFT];
        node->link[REDBLACK_LINK_LEFT] = save->link[REDBLACK_LINK_RIGHT];
        save->link[REDBLACK_LINK_RIGHT] = node;
      }
      node = save;
    }
    free(tree);
  }
}

void* GetFromRedBlackTree(struct RedBlackTree* tree, const void* key)
{
  void* result = NULL;
  struct RedBlackNode* node = tree->root;

  while (node != NULL)
  {
    int result = tree->compare(tree, node, node->key, key);
    if (result == 0)
    {
      // Niddle is found
      break;
    }
    // If the tree supports duplicates, they should be
    // chained to the right sub-tree for this to work
    node = node->link[result < 0];
  }

  if (node != NULL)
  {
    // Get value of current node
    result = node->value;
  }

  return result;
}

int PutIntoRedBlackTree(struct RedBlackTree* tree, const void* key, void* value)
{
  struct RedBlackNode* node = (struct RedBlackNode*)calloc(1, sizeof(struct RedBlackNode));

  if ((tree == NULL) ||
      (node == NULL))
  {
    // Got an error, nothing to do
    return FALSE;
  }

  node->key   = key;
  node->value = value;
  node->color = REDBLACK_COLOR_RED;

  if (tree->root == NULL)
  {
    tree->size = 1;
    tree->root = node;
    tree->root->color = REDBLACK_COLOR_BLACK;
    return TRUE;
  }

  struct RedBlackNode head;   // False tree root
  head.color = REDBLACK_COLOR_BLACK;
  head.link[REDBLACK_LINK_LEFT ] = NULL;
  head.link[REDBLACK_LINK_RIGHT] = tree->root;
  head.key = NULL;
  head.value = NULL;

  struct RedBlackNode* nodeT = &head;
  struct RedBlackNode* nodeG = NULL;

  struct RedBlackNode* nodeQ = tree->root;
  struct RedBlackNode* nodeP = NULL;

  int direction = 0;
  int last      = 0;

  int result = 0;

  // Search down the tree for a place to insert
  do
  {
    if (nodeQ == NULL)
    {
      // Insert node at the first null link
      nodeQ = node;
      nodeP->link[direction] = nodeQ;
    }
    else
    if (IsRed(nodeQ->link[REDBLACK_LINK_LEFT ]) &&
        IsRed(nodeQ->link[REDBLACK_LINK_RIGHT]))
    {
      // Simple red violation: color flip
      nodeQ->color = REDBLACK_COLOR_RED;
      nodeQ->link[REDBLACK_LINK_LEFT ]->color = REDBLACK_COLOR_BLACK;
      nodeQ->link[REDBLACK_LINK_RIGHT]->color = REDBLACK_COLOR_BLACK;
    }

    if (IsRed(nodeQ) &&
        IsRed(nodeP))
    {
      // Hard red violation: rotations necessary
      int direction = (nodeT->link[REDBLACK_LINK_RIGHT] == nodeG);
      if (nodeQ == nodeP->link[last])
        nodeT->link[direction] = RotateRedBlackNodeOnce(nodeG, !last);
      else
        nodeT->link[direction] = RotateRedBlackNodeTwice(nodeG, !last);
    }

    result = tree->compare(tree, nodeQ, nodeQ->key, key);

    if (result != 0)
    {
      last = direction;
      direction = (result < 0);

      // Move the nodes down
      if (nodeG != NULL)
        nodeT = nodeG;

      nodeG = nodeP;
      nodeP = nodeQ;
      nodeQ = nodeQ->link[direction];
    }
  }
  while (result != 0);

  // Update the root (it may be different)
  tree->root = head.link[REDBLACK_LINK_RIGHT];

  // Make the root black for simplified logic
  tree->root->color = REDBLACK_COLOR_BLACK;
  tree->size ++;

  return TRUE;
}

int RemoveFromRedBlackTree(struct RedBlackTree* tree, const void* key, void* value)
{
  if ((tree == NULL) ||
      (tree->root == NULL))
  {
    // Got an error, nothing to do
    return FALSE;
  }

  struct RedBlackNode head;  // False tree root
  head.color = REDBLACK_COLOR_BLACK;
  head.link[REDBLACK_LINK_LEFT ] = NULL;
  head.link[REDBLACK_LINK_RIGHT] = tree->root;
  head.key   = NULL;
  head.value = NULL;

  struct RedBlackNode* nodeQ = &head;
  struct RedBlackNode* nodeP = NULL;
  struct RedBlackNode* nodeG = NULL;
  struct RedBlackNode* found = NULL;  // Found item

  int direction = REDBLACK_LINK_RIGHT;

  // Search and push a red node down
  // to fix red violations as we go
  while (nodeQ->link[direction] != NULL)
  {
    int last = direction;

    // Move the nodes down
    nodeG = nodeP;
    nodeP = nodeQ;
    nodeQ = nodeQ->link[direction];

    int result = tree->compare(tree, nodeQ, nodeQ->key, key);
    direction = (result < 0);

    // Save the node with matching value and keep
    // going; we'll do removal tasks at the end
    if ((result == 0) &&
        ((value == NULL) ||
         (value == nodeQ->value)))
      found = nodeQ;

    // Push the red node down with rotations and color flips
    if (!IsRed(nodeQ) &&
        !IsRed(nodeQ->link[direction]))
    {
      if (IsRed(nodeQ->link[!direction]))
      {
        nodeP->link[last] = RotateRedBlackNodeOnce(nodeQ, direction);
        nodeP = nodeP->link[last];
      }
      else
      if (!IsRed(nodeQ->link[!direction]))
      {
        struct RedBlackNode* nodeS = nodeP->link[!last];
        if (nodeS != NULL)
        {
          if (!IsRed(nodeS->link[!last]) &&
              !IsRed(nodeS->link[ last]))
          {
            // Color flip
            nodeP->color = REDBLACK_COLOR_BLACK;
            nodeS->color = REDBLACK_COLOR_RED;
            nodeQ->color = REDBLACK_COLOR_RED;
          }
          else
          {
            int direction = (nodeG->link[REDBLACK_LINK_RIGHT] == nodeP);

            if (IsRed(nodeS->link[last]))
              nodeG->link[direction] = RotateRedBlackNodeTwice(nodeP, last);
            else
            if (IsRed(nodeS->link[!last]))
              nodeG->link[direction] = RotateRedBlackNodeOnce(nodeP, last);

            // Ensure correct coloring
            nodeQ->color                                             = REDBLACK_COLOR_RED;
            nodeG->link[direction]->color                            = REDBLACK_COLOR_RED;
            nodeG->link[direction]->link[REDBLACK_LINK_LEFT ]->color = REDBLACK_COLOR_BLACK;
            nodeG->link[direction]->link[REDBLACK_LINK_RIGHT]->color = REDBLACK_COLOR_BLACK;
          }
        }
      }
    }
  }

  // Replace and remove the saved node
  if (found != NULL)
  {
    if (tree->release)
    {
      // Call release function before releasing node
      tree->release(tree, found->value);
    }

    found->key   = nodeQ->key;
    found->value = nodeQ->value;

    int direction1 = (nodeP->link[REDBLACK_LINK_RIGHT] == nodeQ);
    int direction2 = (nodeQ->link[REDBLACK_LINK_LEFT ] == NULL);
    nodeP->link[direction1] = nodeQ->link[direction2];

    free(nodeQ);
    tree->size --;
  }

  // Update the root (it may be different)
  tree->root = head.link[REDBLACK_LINK_RIGHT];

  // Make the root black for simplified logic
  if (tree->root != NULL)
    tree->root->color = REDBLACK_COLOR_BLACK;

  return (found != NULL);
}

// RedBlackIterator

// Initialize traversal object, direction determines whether
// to begin traversal at the smallest or largest valued node
void* StartRedBlackIterator(struct RedBlackIterator* iterator, struct RedBlackTree* tree, int direction)
{
  void* result = NULL;

  iterator->tree = tree;
  iterator->node = tree->root;
  iterator->top  = 0;

  // Save the path for later selfersal
  if (iterator->node != NULL)
  {
    while (iterator->node->link[direction] != NULL)
    {
      iterator->path[iterator->top ++] = iterator->node;
      iterator->node = iterator->node->link[direction];
    }
  }

  if (iterator->node != NULL)
  {
    // Get value of current node
    result = iterator->node->value;
  }

  return result;
}

// Traverse a red black tree in the user-specified direction
void* MoveRedBlackIterator(struct RedBlackIterator* iterator, int direction)
{
  void* result = NULL;

  if (iterator->node->link[direction] != NULL)
  {
    // Continue down this branch
    iterator->path[iterator->top ++] = iterator->node;
    iterator->node = iterator->node->link[direction];
    while (iterator->node->link[!direction] != NULL)
    {
      iterator->path[iterator->top ++] = iterator->node;
      iterator->node = iterator->node->link[!direction];
    }
  }
  else
  {
    // Move to the next branch
    struct RedBlackNode* last = NULL;
    do
    {
      if (iterator->top == 0)
      {
        iterator->node = NULL;
        break;
      }
      last = iterator->node;
      iterator->node = iterator->path[-- iterator->top];
    }
    while (last == iterator->node->link[direction]);
  }

  if (iterator->node != NULL)
  {
    // Get value of current node
    result = iterator->node->value;
  }

  return result;
}

void* StartRedBlackIteratorFromValue(struct RedBlackIterator* iterator, struct RedBlackTree* tree, const void* key, int direction)
{
  void* result  = NULL;
  int condition = 0;

  iterator->tree = tree;
  iterator->node = tree->root;
  iterator->top  = 0;

  // Save the path for later selfersal
  if (iterator->node != NULL)
  {
    condition = tree->compare(tree, iterator->node, iterator->node->key, key);
    int direction = (condition < 0);

    while ((condition != 0) &&
           (iterator->node->link[direction] != NULL))
    {
      iterator->path[iterator->top ++] = iterator->node;
      iterator->node = iterator->node->link[direction];

      condition = tree->compare(tree, iterator->node, iterator->node->key, key);
      direction = (condition < 0);
    }
  }

  if (iterator->node != NULL)
  {
    // Get value of current node
    result = iterator->node->value;

    if (((condition < 0) && (direction == REDBLACK_LINK_RIGHT)) ||
        ((condition > 0) && (direction == REDBLACK_LINK_LEFT)))
    {
      // Skip current node if it is out of boundary
      result = MoveRedBlackIterator(iterator, direction);
    }
  }

  return result;
}

int CompareStringKeys(struct RedBlackTree* tree, struct RedBlackNode* node, const void* key1, const void* key2)
{
  return strcmp((const char*)key1, (const char*)key2);
}

int CompareIntegerKeys(struct RedBlackTree* tree, struct RedBlackNode* node, const void* key1, const void* key2)
{
  register int32_t value1 = *(int32_t*)key1;
  register int32_t value2 = *(int32_t*)key2;
  return value1 - value2;
}

int CompareLongIntegerKeys(struct RedBlackTree* tree, struct RedBlackNode* node, const void* key1, const void* key2)
{
  register int64_t value1 = *(int64_t*)key1;
  register int64_t value2 = *(int64_t*)key2;
  return (value1 > value2) - (value1 < value2);
}
