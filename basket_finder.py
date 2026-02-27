import requests
import time
from contextlib import suppress
import json


ids = {}
with open("res.csv", "r+") as file:
    for line in file.readlines():
        new_id = line.split(",")
        with suppress(Exception):
            ids[int(new_id[0]) // 100_000] = int(new_id[0])


sorted_vols = sorted(ids.keys())

print(ids[12])

class StealthMapper:
    def __init__(self, vol_to_art):
        self.vol_to_art = vol_to_art
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)'
        })
        self.mapping = {} # Итог: {vol: basket}

    def check(self, vol_idx, basket_idx):
        """Проверка: принадлежит ли vol под индексом vol_idx баскету basket_idx"""
        if vol_idx >= len(sorted_vols):
            return False
            
        vol = sorted_vols[vol_idx]
        article = self.vol_to_art[vol]
        part = article // 1000
        b_str = f"{basket_idx:02d}"
        url = f"https://basket-{b_str}.wbbasket.ru/vol{vol}/part{part}/{article}/images/big/1.webp"
        
        try:
            time.sleep(0.1) # Стелс-пауза
            return self.session.head(url, timeout=3).status_code == 200
        except Exception:
            return False

    def find_basket_for_vol(self, vol_idx):
        """Линейно ищем номер баскета для конкретной точки"""
        for b in range(1, 100): # Пробуем все возможные баскеты
            if self.check(vol_idx, b):
                return b
        return None

    def run(self):
        v_idx = 0
        while v_idx < len(sorted_vols):
            current_vol = sorted_vols[v_idx]
            
            basket = self.find_basket_for_vol(v_idx)
            
            if not basket:
                print(f"⚠️ Не удалось найти баскет для vol {current_vol}")
                v_idx += 1
                continue

            low = v_idx
            jump = 1
            while v_idx + jump < len(sorted_vols) and self.check(v_idx + jump, basket):
                low = v_idx + jump
                jump *= 2
            
            high = min(v_idx + jump, len(sorted_vols) - 1)

            boundary_idx = low
            while low <= high:
                mid = (low + high) // 2
                if self.check(mid, basket):
                    boundary_idx = mid
                    low = mid + 1
                else:
                    high = mid - 1
            
            # Записываем результат для всех vol в этом диапазоне
            found_basket_str = f"{basket:02d}"
            for i in range(v_idx, boundary_idx + 1):
                self.mapping[sorted_vols[i]] = found_basket_str
                
            print(f"✅ Баскет {found_basket_str} обслуживает vol с {sorted_vols[v_idx]} по {sorted_vols[boundary_idx]}")
            
            # Переходим к следующему vol за границей
            v_idx = boundary_idx + 1

mapper = StealthMapper(ids)
mapper.run()

with open("basket_map.json", "w", encoding="utf-8") as f:
    json.dump(mapper.mapping, f, indent=4)

print(f"🎉 Карта баскетов сохранена! Уникальных групп (vol): {len(mapper.mapping)}")
