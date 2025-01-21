# OOP inheritance

#1
class Parents:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def get_age(self):
        print(self.age)

    def get_name(self):
        print(self.name)

class Childs(Parents):
    def __init__(self, name, age, weight):
        super().__init__(name, age)
        self.weight = weight


    def get_age(self):
        super().get_name()
        print(self.age)


parent1 = Parents("Joe", "50")
child1 = Childs("Radik", 10, 25)

child1.get_age()
#child1.get_name()

#2
class Parents:
    def __init__(self, age):
        print("Parents.__init__")

    def get_age(self):
        print("Parents.get_age")


class Child1(Parents):
    def __init__(self, age):
        print("Child1.__init__")
        super().__init__(age)


class Child2(Parents):
    def __init__(self, age):
        print("Child2.__init__")
        super().__init__(age)


child1 = Child1(10)
