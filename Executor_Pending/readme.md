[new updates]
1. Directly send modification request from here instead of pushing it to modification queue
2. Make a redis bitmap for every sent order and set it to false, when order is filled set it to true and check this bitmap for additional modification.

