'''
Создадим альтернативную версию “Дурака” и проведём 10к игр. После чего посмотрим по ним общую статистику.
52 карты в колоде. Перед каждой игрой перемешивается, то есть будет играться одна из 52! версий сценария
Старшинство определяется по следующим правилам:
    1. иерархия мастей: ♠ < ♣ < ♦ < ♥ (пики < крести < бубны < черви)
    2. по равенству значений внутри одной масти
На руке при наличии карт в колоде у игрока минимум 10 карт.


1. Мешаем колоду.
2. Игроки берут сверху по 10 карт.
3. Стратегия игроков:
    3.1. Игрок 1 использует в качестве первой атаки минимальную карту с руки
    3.1. Игрок 2 использует в качестве первой атаки случайную карту с руки
4. Игрок-1 ходит первым:
    4.1. игрок-1 выкладывает карту
    4.2. игрок-2 пытается бить карту, используя правила старшинства карт
    4.3. Если игрок-2 не может побить карту, то он проигрывает/забирает себе
    4.4. Если игрок-2 бьет карту, то игрок-1 может подкинуть карты любого значения, которое есть на столе.
    4.5. Если игрок-2 может отбить новые карты, то переходим к пункту 4.4
    4.6. Если игрок-2 не может отюить карты, то он проигрывает/забирает все со стола
5. При необходимости карты добираются из колоды
6. Если Игрок-2 отбился, то Игрок-1 и Игрок-2 меняются местами. Игрок-2 ходит, Игрок-1 отбивается.

NB в среднем 7 случаях из 10000 происходит зацикливание (ходов больше 1000), будем считать, что игроки договорились на ничью.
А количество раундов в таком случае возьмём как медиану из уже собранного распределения раундов.
По итогу множественных симуляций было получено, что при раундах > 100 (таких ~16), в половине будет зацикливание. Поэтому 100 будет границей.

При запуске симуляция независимо от предыдущих игр чередуется право первого хода у 1 и 2 игрока.

Нулевая гипотеза: стратегии выбора первой карты 1 и 2 игрока дают одинаковую продолжительность игры.
Альтернативная: стратегии выбора первой карты 1 и 2 игрока дают разную продолжительность игры.
'''


from __future__ import annotations
import random

import seaborn as sns
import matplotlib.pyplot as plt
from collections import Counter
import numpy as np
from scipy import stats

# исходники для колоды 52 карты
VALUES = ('2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A')
SUITS = ('Spades', 'Clubs', 'Diamonds', 'Hearts')
SUITS_UNI = {
        'Spades': '♠',
        'Clubs': '♣',
        'Diamonds': '♦',
        'Hearts': '♥'
}

# класс Карты из колоды
class Card:
    def __init__(self, value: str, suit: str) -> None:
        self.value = value  # Значение карты(2, 3... 10, J, Q, K, A)
        self.suit = suit    # Масть карты
        self.value_i = VALUES.index(value)  # вес значения
        self.suit_i = SUITS.index(suit)  # вес масти

    # определяем метод печати экземляр класса
    def __str__(self) -> str:
        return f'{self.value}{SUITS_UNI[self.suit]}'

    # проверка принадлежности к классу карта сравниваемого объекта
    def _check_type(self, other_card: Card):
        if type(self) == type(other_card):
            return True
        raise TypeError('Принимаются значения только одного типа')

    # проверка на одномастность
    def equal_suit(self, other_card: Card) -> bool:
        return self.suit_i == other_card.suit_i and self._check_type(other_card)

    # проверка на равнозначность
    def equal_value(self, other_card: Card) -> bool:
        return self.value_i == other_card.value_i and self._check_type(other_card)

    # определение метода меньше "<"
    def __lt__(self, other_card: Card) -> bool:
        if self._check_type(other_card):
            if self.equal_suit(other_card):
                return self.value_i < other_card.value_i
            return self.suit_i < other_card.suit_i

    # определение метода больше ">"
    def __gt__(self, other_card: Card) -> bool:
        return not self < other_card

    # определение метода равно "="
    def __eq__(self, other):
        return self.value_i == other.value_i and self.suit_i == other.suit_i and self._check_type(other)



# Класс колоды из 52-ух карт
class Deck:
    def __init__(self, cards=None, start=0) -> None:
        # Список карт в колоде. Каждым элементом списка будет объект класса Card
        if cards is None:
            cards = [Card(value, suit) for suit in SUITS for value in VALUES]

        self.cards = cards
        self.size = len(cards)  # Текущий размер калоды
        self.start = start - 1  # Начальная позиция итератора

    # Метод печати экземпляра класса
    def __str__(self) -> str:
        list_card = ''
        for card in self.cards:
            list_card += (str(card) + ', ')
        return f"deck[{self.size}]: {list_card}"

    # Возвращает x первых карт из колоды в виде списка, эти карты убираются из колоды
    def draw(self, x: int) -> list:

        if x <= self.size:
            self.size -= x
            new_card = self.cards[:x]
            self.cards = self.cards[x:]
            return new_card
        else:
            new_card = self.cards
            self.cards = []
            self.size = 0
            return new_card

    # Метод перемешивания колоды
    def shuffle(self) -> None:
        random.shuffle(self.cards)

    # Реализация итератора
    def __iter__(self):
        return self

    def __getitem__(self, idx):
        return self.cards[idx]

    def __next__(self):
        self.start += 1
        if self.start < self.size:
            return self.cards[self.start]
        else:
            raise StopIteration


# Поиск минимальной карты для отбивки
def card_for_play(other_card: Card, hand: list):

    lst = [card for card in hand if card > other_card]
    if lst:
        return min(lst)
    return None


# Поиск карт для подкидывания (подкидываются карты одной велечины)
def cards_for_add(cur_table: list, hand: list, max_size: int):
    lst = []
    for card in hand:
        for t_card in cur_table:
            if card.equal_value(t_card):
                lst.append(card)
                if len(lst) == max_size:  # Проверка по кол-ву карт в руке у отбивающегося
                    return lst
    if lst:
        return lst
    return None


# Поиск карт для отбивки нескольких карт
def cards_for_second_play(attack_cards: list, hand: list):
    lst = []
    for attack in attack_cards:
        card = card_for_play(attack, hand)  # Функция поиска отбивки для одной карты

        # Если не смогли отбить хоть одну карту, считаем, что не можем отбить все
        if not card:
            return None

        lst.append(card)
        hand = [card_hand for card_hand in hand if not card_hand == card]  # Убираем с руки карту, которой уже отбили
    return lst


# Добор из колодыс помощью метода Deck.drow()
def get_cards(cur_deck: Deck, player: list):
    if cur_deck.size != 0 and len(player) < 10:
        for card in cur_deck.draw(10 - len(player)):
            player.append(card)


# Имитация игры
def play_game(deck=Deck(), whose_turn_flg=True):

    # игроки берут начальные 10 карт
    player1 = deck.draw(10)
    player2 = deck.draw(10)
    table = []  # Сущность стола

    step = 1  # Номер раунда
    attacker = whose_turn_flg  # Флаг = True - аттакует 1 игрок, в конце каждого раунда меняется на противоположное значение

    # играем, пока у игроков есть карты
    while (player1 and player2):
        # Определяем аттакующего игрока
        if attacker:
            attack_player, defense_player = player1, player2
        else:
            attack_player, defense_player = player2, player1

        # Определяем стратегию хода игроков
        if attacker:
            attack_card = min(attack_player)
        else:
            attack_card = random.choice(attack_player)

        attack_player = [card for card in attack_player if not card == attack_card]  # Убираем выложенную карту с руки

        table.append(attack_card)  # Кладем карту на стол
        defense_card = card_for_play(attack_card, defense_player)  # Игрок ищет, чем отбить

        if not defense_card:
            # Если отбить нечем, то раунд заканчивается

            get_cards(deck, attack_player)  # Добор из колоды (при необходимости)
            defense_player.append(table[0])  # Взятие со стола
        else:
            # У игрока есть, чем отбиться

            defense_player = [card for card in defense_player if not card == defense_card]  # Убираем карту с руки
            table.append(defense_card)  # Кладем карту на стол

            # Игроки подкидывают и отбиваются, пока это возможно
            while(True):
                # Аттакующий ищет, что подкинуть
                seq_attack_carts = cards_for_add(table, attack_player, len(defense_player))

                if not seq_attack_carts:
                    # Подкинуть нечего, игроки добирают карты из колоды (при необходимости). Конец раунда
                    get_cards(deck, attack_player)
                    get_cards(deck, defense_player)
                    break
                else:
                    # Аттакующий подкидывает карты на стол и убирает со своей руки
                    attack_player = [card for card in attack_player if card not in seq_attack_carts]
                    for card in seq_attack_carts:
                        table.append(card)

                    # Отбивающийся проверяет, сможет ли покрыть атаку
                    seq_defense_cards = cards_for_second_play(seq_attack_carts, defense_player)

                    if not seq_defense_cards:
                        # Покрыть нечем. Отбивающийся забирает карты со стола
                        for card in table:
                            defense_player.append(card)
                            # Аттакующий берет из колоды (при необходимости). Конец раунда
                        get_cards(deck, attack_player)
                        break

                    else:
                        # Отбивающий покрыл аттаку
                        defense_player = [card for card in defense_player if card not in seq_defense_cards]
                        # Новые карты добавлены на стол
                        for card in seq_defense_cards:
                            table.append(card)

        '''
        В конце каждого раунда:
            * У сущности игрока обновляется информация о состоянии карт на руке.
            * Меняется флаг аттакующего
            * Номер раунда увеличивается на 1
            * Стол очищается
        '''
        if attacker:
            player1, player2 = attack_player, defense_player
        else:
            player2, player1 = attack_player, defense_player

        attacker = not attacker
        step = step + 1
        table = []

        # Защита от зацикливания
        if step >= 100:
            step = 100
            player1, player2 = [], []
            break

    '''
    Возвращаемый результат функции игры ФЛАГ результата:
        * 0 - ничья
        * 1 - победил игрок 1
        * 2 - победил игрок 2
    И КОЛИЧЕСТВО раундов
    '''
    if len(player1) == 0 and len(player2) == 0:
        return 0, step
    elif len(player1) == 0:
        return 1, step
    return 2, step

# Проверка Т-тестом Стьюдента
def t_test(samp1, samp2):
    p_val = stats.ttest_ind(samp1, samp2,
                            equal_var=False)[1]

    print(f'Т-тест Стьюдента дал p_value = {p_val}')
    print(
        f'Нулевая гипотеза отвергается, выборки принадлежат разным ГС' if p_val < 0.05 else f'Нулевая гипотеза принимается, выборки принадлежат одной ГС')


results = []  # Список результатов игр
steps = []  # Распрделение количества раундов
steps_no_winner = []  # Распрделение количества раундов с ничьей
steps_winner_1 = []  # Распрделение количества раундов с победой игрока 1
steps_winner_2 = []  # Распрделение количества раундов с победой игрока 2

whose_turn = True  # Чередование хода в начале игры

# Имитация 10к игр с сохранением результатов
for i in range(10000):

    deck_s = Deck()
    deck_s.shuffle()

    res, step_val = play_game(deck_s, whose_turn)
    whose_turn = not whose_turn

    # Ничья при зацикливании
    if step_val == 100:
        step_val = np.median(steps)

    results.append(res)
    steps.append(step_val)

    if res == 0:
        steps_no_winner.append(step_val)
    elif res == 1:
        steps_winner_1.append(step_val)
    else:
        steps_winner_2.append(step_val)

# Проверяем выборки Т-тестом, тк они имеют визуально нормальное распределение и большой объём
t_test(steps_winner_1, steps_winner_2)        

# Считаем количество каждого исхода
cnt = dict(Counter(results))

# Отрезаю хвост, больше 45 для наглядности побед 1 игрока
steps_winner_1_40 = [step for step in steps_winner_1 if step <= 40]
steps_winner_2_40 = [step for step in steps_winner_2 if step <= 40]

# Строим отчёт по общей статистике игр
sns.set()
fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(16, 8))

sns.histplot(steps, kde=True, ax=axes[0, 0], color='#013220', binwidth=1)
sns.histplot(steps_winner_1_40, kde=True, ax=axes[1, 0], color='#013220', binwidth=1)
sns.histplot(steps_winner_2_40, kde=True, ax=axes[1, 1], color='#013220', binwidth=1)
sns.histplot(steps_no_winner, kde=True, ax=axes[2, 0], color='#013220', binwidth=1)

my_pal_tags = ["#aac1bf", "#aac1b2", "#aac1bf"]
my_pal = sns.set_palette(palette=my_pal_tags)
sns.barplot([*cnt], [*cnt.values()], ax=axes[0, 1], palette=my_pal, edgecolor='#013220')

fig.suptitle('Игрок 1 ходит первой картой - минимальной с руки, игрок 2 - случайной')
axes[0, 0].set_title('Количество раундов')
axes[0, 1].set_title('Распределение результатов')
axes[1, 0].set_title('Победа игрока 1, раундов <= 40')
axes[1, 1].set_title('Победа игрока 2, раундов <= 40')
axes[2, 0].set_title('Ничья')

mesg = "По итогам 10к игр.\n" + \
    f"В среднем сыграно раундов: {np.mean(steps):.0f}\n\tПри ничье: {np.mean(steps_no_winner):.0f}.\n" + \
       f"\tПри победе игрока 1: {np.mean(steps_winner_1):.0f}.\n\tПри победе игрока 2: {np.mean(steps_winner_2):.0f}." + \
    f"\nБольше 40 раундов при победе 1 | 2 игрока: {len([step for step in steps_winner_1 if step > 40])} | {len([step for step in steps_winner_2 if step > 40])} раз."
axes[2, 1].text(x=0.05, y=0.13, s=mesg, fontsize=16, color='#013220')
axes[2, 1].grid(False)
axes[2, 1].get_xaxis().set_visible(False)
axes[2, 1].get_yaxis().set_visible(False)

fig.tight_layout()
plt.show()
