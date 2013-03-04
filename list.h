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

#ifndef __LIST_H__
#define __LIST_H__
#include <assert.h>

#define LIST_INIT(list) (list)->next = (list)->prev = (list); (list)->data = NULL;

#define LIST_ISEMPTY(head) ( (head)->next == (head))

#define LIST_ADD_BEFORE(_new,_old) \
   do {\
      ListNode* new= _new ,*old =_old;\
      (new)->next = (old);\
      (new)->prev = (old)->prev;\
      (old)->prev->next = (new);\
      (old)->prev = (new);\
   }while(0);

#define LIST_ADD_AFTER(new,old) \
   (new)->prev = (old);\
   (new)->next = (old)->next;\
   (old)->next = (new);\
   (new)->next->prev = (new);


#define LIST_FIRST(head) ((head)->next)
#define LIST_LAST(head) (head)->prev

#define LIST_FOR_ALL(head,tmp)\
   for (tmp = (head)->next; tmp != (head); tmp = tmp->next)

#define LIST_FOR_ALL_SAFE(head,elem,_next) \
	for ((elem) = (head)->next, (_next) = (elem)->next; (elem) != (head) ; (elem) = (_next),(_next) = (_next)->next)

/*static void NoFree() {

}

#define NOFREE NoFree*/

#define LIST_REMOVE(head,elem)\
	do {\
		(elem)->next->prev = (elem)->prev;\
		(elem)->prev->next = (elem)->next;\
	} while(0)

	

#define LIST_DESTROY(head,freeFn) \
	do {\
		ListNode *_elem = NULL , *vvv = NULL;\
		LIST_FOR_ALL_SAFE((head),_elem,vvv) {\
			LIST_REMOVE(head,_elem);\
			if ( 1 && (freeFn) )  {\
				((void (*)(void*))(freeFn))(_elem->data);\
			}\
			free(_elem);\
		}\
	} while(0)

typedef struct ListNode {
   struct ListNode* next;
   struct ListNode* prev;
   void* data;
} ListNode;

#endif
