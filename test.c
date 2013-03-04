/*
    Copyright (C) 2013 Sridhar Valaguru <sridharnitt@gmail.com>

    This file is part of MorphDB.

    MorphDB is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    MorphDB is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with MorphDB.  If not, see <http://www.gnu.org/licenses/>.
*/


static uint8_t 
MorphDBIntNodeCopy(BlockMgrHandlePtr dbHandle,MorphDBIntNode* src,
                   MorphDBIntNode** outNode) {
   MorphDBIntNode *node = NULL;

   node = malloc(sizeof(MorphDBIntNode));
   if (!node) {
      dbHandle->error = MORPHDB_NO_MEMORY;
      return False;
   }
   node->desc = malloc(src->desc->totalLength);
   if (!node->desc) {
      dbHandle->error = MORPHDB_NO_MEMORY;
      return False;
   }
   memcpy(node->desc,src->buf,src->desc->totalLength);
   node->parent = NULL;
   node->keyPtrs = node->desc->keyPtrs;
   node->ptrPtrs = node->desc->ptrPtrs;
   node->buf = node->desc;
   *outNode = node;
   return True;
}

uint8_t 
MorphDBBTreeCursorSetToStart(MorphDBBTreeCursor* cursor) {
   return MorphDBBTreeCursorSetPos(cursor,cursor->startKey);
}

static uint8_t 
MorphDBBTreeCursorSetPosInt(MorphDBBTreeCursor* cursor,BTreeKey key,uint8_t *curKeyChanged) {
   BlockMgrHandlePtr dbHandle = cursor->db;
   BTreeKey cur;
   BTreeKey newKey;
   MorphDBIntNode* node = NULL;

   if (key.data == NULL) {

      cursor->curIdx = 0;
      node = DBTreeGetRoot(dbHandle);
      while (!ISLEAF(node)) {
         if (!DBTreeLoadNode(dbHandle,*PTR_DATA(node,0,TORDER(dbHandle)),node,&node)) {
            cursor->error = dbHandle->error;
            return False;
         }
      }
   } else {
      if (!DBTreeFindLeaf(dbHandle,&key,&node)){
         cursor->error = dbHandle->error;
         return False;
      }
      cursor->curIdx = DBTreeNodeBinSearch(dbHandle,node,&key);
   }
   
   KEY_TO_BDATA(dbHandle,node,cursor->curIdx,&cur);
   if (!BTreeKeyCopy(dbHandle,&newKey,&cur,True)) {
      cursor->error = MORPHDB_NO_MEMORY;
      return False;
   }
   if (curKeyChanged) {
      if (BTreeKeyCmp(dbHandle,&cursor->curKey,&newKey)){
         *curKeyChanged = True;
      } else {
         *curKeyChanged = False;
      }
   }
   BinaryStrFree(&cursor->curKey);
   cursor->curKey = newKey;
   if (cursor->curNode) {
      MorphDBIntNodeFree(cursor->curNode);
   }
   if (!MorphDBIntNodeCopy(dbHandle,node,&cursor->curNode)) {
      cursor->error = MORPHDB_NO_MEMORY;
      return False;
   }
   return True;
}

uint8_t 
MorphDBBTreeCursorSetPos(MorphDBBTreeCursor* cursor,BTreeKey key) {
   return MorphDBBTreeCursorSetPosInt(cursor,key,NULL);
}

uint8_t 
MorphDBBTreeCursorInit(BlockMgrHandlePtr dbHandle,MorphDBBTreeCursor** cursor) {
   MorphDBBTreeCursor* cur;

   cur = malloc(sizeof(MorphDBBTreeCursor));
   if (!cur) {
      dbHandle->error = MORPHDB_NO_MEMORY;
      return False;
   }
   memset(cur,0,sizeof(*cur));
   cur->startKey.data = NULL;
   cur->startKey.len = 0;

   cur->endKey.data = NULL;
   cur->endKey.len = 0;

   cur->db = dbHandle;
   if (!MorphDBBTreeCursorSetToStart(cur)) {
      return False;
   }
   *cursor = cur;
   return True;
}

uint8_t 
MorphDBBTreeCursorSetStartKey(MorphDBBTreeCursor* cursor,BTreeKey key) {
   BinaryStrFree(&cursor->startKey);
   if (BinaryStrDup(&cursor->startKey,&key)) {
      cursor->error = MORPHDB_NO_MEMORY;
      return False;
   }
   if (!MorphDBBTreeCursorSetToStart(cursor)) {
      return False;
   }
   return True;
}

uint8_t 
MorphDBBTreeCursorSetEndKey(MorphDBBTreeCursor* cursor,BTreeKey key) {
   BinaryStrFree(&cursor->endKey);
   if (BinaryStrDup(&cursor->endKey,&key)) {
      cursor->error = MORPHDB_NO_MEMORY;
      return False;
   }
   return True;
}

uint8_t 
MorphDBBTreeCursorValue(MorphDBBTreeCursor* cursor,
                   BTreeKey* key,
                   BTreeValue* value) {
   BTreeKey cur;

   if (cursor->gen != cursor->db->gen) {
      if (!MorphDBBTreeCursorSetPos(cursor,cursor->curKey)) {
         return False;
      }
      cursor->gen = cursor->db->gen;
   }
   if (cursor->curIdx >= NKEYS(cursor->curNode)) {
      // no more data
      cursor->error = MORPHDB_CURSOR_END;
      return False;
   }
   KEY_TO_BDATA(cursor->db,cursor->curNode,cursor->curIdx,&cur);
   if (!BTreeKeyCopy(cursor->db,key,&cursor->curKey,True)) {
      cursor->error = MORPHDB_NO_MEMORY;
      return False;
   }

   if (!BlockMgrDataRead(cursor->db->handle,
                        *PTR_DATA(cursor->curNode,cursor->curIdx,TORDER(cursor->db)),value)) {
      cursor->error = cursor->db->handle->error;
      return False;
   }
   return True;
}

uint8_t 
MorphDBBTreeCursorNext(MorphDBBTreeCursor* cursor) {
   uint8_t curKeyChanged=  False;
   BlockMgrHandlePtr db = cursor->db;
   MorphDBIntNode* node;
   BTreeKey cur;

   if (cursor->gen != db->gen) {
      if (!MorphDBBTreeCursorSetPosInt(cursor,cursor->curKey,&curKeyChanged)) {
         return False;
      }
      cursor->gen = db->gen;
   }
   if (curKeyChanged) {
      if (cursor->curIdx >= NKEYS(cursor->curNode)) {
         // no more data
         cursor->error = MORPHDB_CURSOR_END;
         return False;
      }
      // already moved to next position
      return True;
   }
   if ((cursor->curIdx >= (NKEYS(cursor->curNode)-1))) {
      if (BlockMgrIsNullPtr(&LNEXT(cursor->curNode))) {
         cursor->error = MORPHDB_NO_MEMORY;
         return False;
      }
      if (!DBTreeLoadNode(db,LNEXT(cursor->curNode),NULL,&node)) {
         cursor->error = db->error;
         return False;
      }
      // XXX 
      MorphDBIntNodeFree(cursor->curNode);
      if (!MorphDBIntNodeCopy(db,node,&cursor->curNode)) {
         cursor->error = MORPHDB_NO_MEMORY;
         return False;
      }
      //cursor->curNode = node;
      cursor->curIdx = 0;
   } else {
      cursor->curIdx++;
   }

   KEY_TO_BDATA(db,cursor->curNode,cursor->curIdx,&cur);
   if (cursor->endKey.data && BTreeKeyCmp(db,&cur,&cursor->endKey)>0) {
      cursor->error = MORPHDB_CURSOR_END;
      return False;
   }
   if (!BTreeKeyCopy(cursor->db,&cursor->curKey,&cur,True)) {
      cursor->error = MORPHDB_NO_MEMORY;
      return False;
   }
   return True;
}
