from abc import ABC, abstractmethod


class Character(ABC):
    def __init__(self, name, health_points, attack_points):
        self.name = name
        self.health_points = health_points
        self.attack_points = attack_points

    @abstractmethod
    def attack(self, other):
        if other.health_points == 0:
            print("Already dead")
        elif other.health_points - self.attack_points < 0:
            print(f"{self.name} attacks {other.name} with {self.attack_points} points.")
            other.health_points = 0
        else:
            print(f"{self.name} attacks {other.name} with {self.attack_points} points.")
            other.health_points -= self.attack_points


class Hero(Character):
    def __init__(self, name, health_points, attack_points, hero_type):
        super().__init__(name, health_points, attack_points)
        self.hero_type = hero_type

    def attack(self, other):
        return super().attack(other)


class Enemy(Character):
    def attack(self, other):
        return super().attack(other)


class CharacterFactory:
    def create_character(self, character_type, name, health_points, attack_points, hero_type=None):
        if character_type == 'hero':
            return Hero(name, health_points, attack_points, hero_type)
        elif character_type == 'enemy':
            return Enemy(name, health_points, attack_points)
        else:
            raise ValueError("undetectable character_type")


# Код ниже не трогаем! Нужен для проверки!
if __name__ == '__main__':
    factory = CharacterFactory()
    hero = factory.create_character('hero', 'John', 100, 20, 'Warrior')
    enemy = factory.create_character('enemy', 'Goblin', 50, 10)

    while hero.health_points > 0 and enemy.health_points > 0:
        hero.attack(enemy)
        enemy.attack(hero)

    if hero.health_points > 0:
        print(f'{hero.name} wins with {hero.health_points} health points remaining!')
    else:
        print(f'{enemy.name} wins wi    th {enemy.health_points} health points remaining!')
