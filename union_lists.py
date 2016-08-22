list1 = [1, 2, 3, 4, 5, 6, 7]
list2 = [4, 5, 6, 7, 8]

union_list = []

while list1 and list2:
    if list1[0] < list2[0]:
        union_list.append(list1.pop(0))
    elif list1[0] > list2[0]:
        union_list.append(list2.pop(0))
    else:
        union_list.append(list1.pop(0))
        list2.pop(0) # remove duplicate

while list1:
    union_list.append(list1.pop(0))

while list2:
    union_list.append(list2.pop(0))    
    
union_list
