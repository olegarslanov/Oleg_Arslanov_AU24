class Monkey:
    """Just a little monkey."""
    banana_count = 5

    def __init__(self, name):
        self.name = name

    def greet(self):
        print(f'Hi, I am {self.name}!')

    def eat_banana(self):
        if self.banana_count > 0:
            self.banana_count -= 1
            print('Yammy!')
        else:
            print('Still hungry :(')

monkey1 = Monkey("Milada")

monkey1.greet()

monkey1.eat_banana()

print(monkey1.banana_count)

print(dir())


#Inheritance

class Ancestor:
    def __init__(self):
        print("Ancestor.__init__")

    def fun(self):
        print("Ancestor.fun")

    def work(self):
        print("Ancestor.work")


class Child(Ancestor):
    def __init__(self):
        print("Child.__init__")
    def fun(self):
        print("Child.fun")

c = Child()
c.fun()
c.work()

print(type(monkey1))
print(dir())


# ISSUBCLASS, ISINSTANCE, TYPE (funkcij dlja nahozdenija zavisimostej mezdu class)


class A:
    pass

class B(A):
    pass

class C:
    pass

print(issubclass(B, A))

print(issubclass(A, B))

print(issubclass(A, C))
