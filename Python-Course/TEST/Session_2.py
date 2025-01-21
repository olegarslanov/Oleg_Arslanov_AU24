#HASH

class Dog():
    pass

print(hash(5.2))
print(hash('string'))
print(hash((1, 2, 3)))
print(hash(Dog()))
print(hash(sum))
#print(hash([1, 2, 3]))


#DICTIONARY


my_dict = {'Name': 'Zara', 'Name': 'Manni'} # key is unique
print(my_dict)



#SET

my_set = {'C'}

my_set.add('Python')
print(my_set)

my_set.update({'Go', 'Rust'})
print(my_set)

my_set.remove('Rust')
print(my_set)

my_set.remove('Go')
print(my_set)


fib = {1,2,3,5,8,13}
prime = {2,3,5,7,11,13}

fib | prime # Union
{1, 2, 3, 5, 7, 8, 11, 13}

fib & prime # Intersection
{2, 3, 5, 13}

fib ^ prime # Symmetric Difference
{1, 7, 8, 11}


frozen_set = {1, 2, 3} # frozen set ne dobavish nichego
print(frozen_set.add(5))


# FUNCTIONS


#raspakovka ... nado zapolnit po liubomu vse elementy, daze zabiraja iz list i dict
def func(a, b, c, d=False, *args, **kwargs):
    print(a, b, c, d, args, kwargs)

func(*[1,2,3,4,5], **{'6':7})

func(*[1,2,3,], **{'d':7})

func(1, 2, *[3,], **{'d':7})



# UNPACKING ITERABLES

gen = (i ** 2 for i in range(3))
a, b, c = gen
print(a, b,c)

my_tuple = (1, 2, 3)
ss = (0, *my_tuple, 4)
print(ss)

