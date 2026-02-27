import json as js
from asyncio import Semaphore, create_task, gather, sleep, run
from pathlib import Path

import pandas as pd
from aiohttp import ClientSession
from aiohttp.client_exceptions import ConnectionTimeoutError
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

### CONFIG
HEADERS: dict = {"Cookie": ""}
SEMAPHORES: int = 15
query = "пальто из натуральной шерсти"
JSON_FILE = "res.json"

RANGES = {
    "01": range(12, 141),
    "02": range(144, 275),
    "03": range(291, 435),
    "04": range(437, 720),
    "05": range(724, 1005),
    "06": range(1022, 1039),
    "07": range(1077, 1116),
    "08": range(1126, 1167),
    "09": range(1182, 1311),
    "10": range(1316, 1595),
    "11": range(1612, 1638),
    "12": range(1657, 1915),
    "13": range(1922, 2043),
    "14": range(2049, 2188),
    "15": range(2193, 2406),
    "16": range(2412, 2616),
    "17": range(2625, 2838),
    "18": range(2841, 3045),
    "19": range(3054, 3264),
    "20": range(3271, 3485),
    "21": range(3499, 3698),
    "22": range(3704, 3897),
    "23": range(3952, 4124),
    "24": range(4138, 4340),
    "25": range(4354, 4566),
    "26": range(4570, 4869),
    "27": range(4883, 5170),
    "28": range(5192, 5502),
    "29": range(5503, 5814),
    "30": range(5814, 6125),
    "31": range(6126, 6438),
    "32": range(6483, 6725),
    "33": range(6759, 7058),
    "34": range(7064, 7373),
    "35": range(7381, 7684),
    "36": range(7684, 7994),
    "37": range(8003, 8305),
    "38": range(8319, 8570)
}


def get_basket(article: int):
    id = article // 100_000
    for basket, ran in RANGES.items():
        if id in ran:
            return basket
    else:
        print(f"Basket не найден {article}")
        raise TimeoutError
    


class ResData(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    article: int = Field(validation_alias="id")
    article_link: str = ""

    seller_name: str = Field(validation_alias="supplier")
    brandId: int | None = None
    seller_link: str = ""

    name: str
    sizes: list[dict] = Field(default_factory=list)
    sizes_res: str = ""
    price: int = 0
    cnt_left: int = 0

    review_rating: float = Field(validation_alias="reviewRating")
    feedbacks: int

    @model_validator(mode="after")
    def generate_links(self) -> "ResData":
        self.article_link = f"https://www.wildberries.ru/catalog/{self.article}/detail.aspx"
        if self.brandId:
            self.seller_link = f"https://www.wildberries.ru/brands/{self.brandId}"
        return self

    @model_validator(mode="after")
    def fill_sizes_data(self) -> "ResData":
        self.sizes_res = ",".join(obj.get("name", "") for obj in self.sizes)
        self.cnt_left = sum(
            int(obj["stocks"][0].get("qty", 0))
            for obj in self.sizes
            if obj.get("stocks")
        )

        for prod in self.sizes:
            price = prod.get("price", {})
            value = price.get("product", 0) if price else 0
            if value:
                self.price = value // 100
                break

        return self
    

class CardData(BaseModel):
    model_config = ConfigDict(extra="ignore")

    description: str = ""
    link: str
    media: dict | None = None
    pic_links: str = ""
    options: list[dict] | None = None
    characteristics: str = ""

    @model_validator(mode="after")
    def fill_pic_links(self) -> "CardData":
        if self.media is None:
            return self

        cnt: int = self.media.get("photo_count", 0)
        link_base = "/".join(self.link.split("/")[:-3] + ["images", "big"])

        self.pic_links = ",".join(
            f"{link_base}/{i}.webp"
            for i in range(1, cnt + 1)
        )
        return self

    @model_validator(mode="after")
    def fill_characteristics(self) -> "CardData":
        if self.options is None:
            return self

        self.characteristics = "\n".join(
            f"{c.get('name')}: {c.get('value')}"
            for c in self.options
        )
        return self
    


@retry(
    stop=stop_after_attempt(5), 
    wait=wait_fixed(2),
    retry=retry_if_exception_type(ConnectionTimeoutError)
)
async def get_card(
    id: int,
    session: ClientSession,
    semaphore: Semaphore
):
    async with semaphore:
        
        url = f"https://www.wildberries.ru/__internal/u-card/cards/v4/detail?appType=1&curr=rub&dest=-1255987&spp=30&hide_vflags=4294967296&ab_testing=false&lang=ru&nm={id}"
        async with session.get(url=url) as res:
            if res.status == 429:
                await sleep(1)
                return await get_card(id=id, session=session, semaphore=semaphore)

            json = await res.json()
            try:
                card = json['products'][0]
            except Exception as e:
                print(e, end=" | ")
                print(res.status)
            try:
                model_a = ResData.model_validate(card)
            except ValidationError as e:
                print(e.json())
                print(json)

        await sleep(0.5)

        try:
            basket = get_basket(article=id)
        except TimeoutError:
            return
        
        part = id // 1_000
        vol = id // 100_000

        url = f"https://basket-{basket}.wbbasket.ru/vol{vol}/part{part}/{id}/info/ru/card.json"
        async with session.get(url=url) as res:
            if res.status == 404:
                print(url)
                return

            if res.status == 429:
                await sleep(1)
                return await get_card(id=id, session=session, semaphore=semaphore)
            json = await res.json()
            json["link"] = url
            try:
                model_b = CardData.model_validate(json)
            except ValidationError as e:
                print(json)
                print(e.json())
                return

        model_a_json: dict = model_a.model_dump(exclude={"sizes", "brandId"})
        model_b_json: dict = model_b.model_dump(exclude={"link", "pic_cnt", "options", "media"})

        model_a_json.update(model_b_json)

        with open(JSON_FILE, "a", encoding="utf-8") as f:
            js.dump(model_a_json, fp=f, ensure_ascii=False)
            f.write("\n")

        return model_a_json



async def cards_resolver(
    ids: list[int],
):
    sem = Semaphore(SEMAPHORES)

    async with ClientSession(headers=HEADERS) as session:
        tasks = [create_task(get_card(id=id, session=session, semaphore=sem)) for id in ids]

        await gather(*tasks)
    


@retry(
    stop=stop_after_attempt(5), 
    wait=wait_fixed(5),
    retry=retry_if_exception_type(ConnectionTimeoutError)
)
async def main():
    new_ids = set()

    gen = f"https://www.wildberries.ru/__internal/u-search/exactmatch/ru/common/v18/search?ab_testing=false&appType=1&curr=rub&dest=-1255987&hide_vflags=4294967296&lang=ru&page=1&query={query}&resultset=catalog&sort=popular&spp=30&suppressSpellcheck=false"
    
    exist_ids = set()
    if not Path(JSON_FILE).exists():
        with open(JSON_FILE, "w"):
            pass
    else:
        with open(JSON_FILE, "r+") as f:
            for line in f:
                if line.strip():
                    line_dict: dict = js.loads(line)
                    exist_ids.add(line_dict.get("article"))


    async with ClientSession(headers=HEADERS) as session:
        async with session.get(url=gen) as res:
            text = await res.text()
            t_dict = js.loads(text)

            total = t_dict.get("total", 0)

        for i in range(1, (total // 100) + 2):
            url = f"https://www.wildberries.ru/__internal/u-search/exactmatch/ru/common/v18/search?ab_testing=false&appType=1&curr=rub&dest=-1255987&hide_vflags=4294967296&lang=ru&page={i}&query={query}&resultset=catalog&sort=popular&spp=30&suppressSpellcheck=false"

            async with session.get(url=url) as res:
                text = await res.text()
                t_dict = js.loads(text)

                for pr in t_dict.get("products", []):
                    new_ids.add(pr.get("id", 0))
    print(f"Old {len(exist_ids)} || Curr {len(new_ids)}", end=" || ")

    ids = list(new_ids - exist_ids)
    print(f"Actual {len(ids)}")
    await cards_resolver(ids=ids)



def process_data_from_json(json_filename: str):
    """
    Читает данные из JSON и сохраняет их в два XLSX файла.
    """
    try:
        data_list = []
        with open(json_filename, "r", encoding="utf-8") as f:
            for line in f.readlines():
                data_list.append(js.loads(line))
        
        print(f"Успешно прочитано {len(data_list)} товаров из {json_filename}")
        
        df = pd.DataFrame(data_list)
        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
        df['review_rating'] = pd.to_numeric(df['review_rating'], errors='coerce').fillna(0)

        df = df.rename(columns={
            'article': 'Артикул',
            'article_link': 'Ссылка на товар',
            'name': 'Название',
            'price': 'Цена',
            'description': 'Описание',
            'pic_links': 'Ссылки на изображения',
            'characteristics': 'Характеристики',
            'seller_name': 'Продавец',
            'seller_link': 'Ссылка на продавца',
            'sizes_res': 'Размеры',
            'cnt_left': 'Остатки',
            'review_rating': 'Рейтинг',
            'feedbacks': 'Количество отзывов'
        })


        df.to_excel("full_catalog.xlsx", index=False)
        print("Полный каталог сохранен в 'full_catalog.xlsx'")

        mask = (
            (df['Рейтинг'] >= 4.5) & 
            (df['Цена'] <= 10000) & 
            (df['Характеристики'].str.contains("Россия", case=False, na=False))
        )
        
        filtered_df = df[mask]
        filtered_df.to_excel("russia_top_selection.xlsx", index=False)
        print(f"Выборка сохранена в 'russia_top_selection.xlsx'. Найдено: {len(filtered_df)}")

    except FileNotFoundError:
        print(f"Ошибка: Файл {json_filename} не найден.")
    except Exception as e:
        print(f"Произошла ошибка: {e}")


if __name__ == "__main__":
    try:
        run(main())
    except Exception:
        pass
    finally:
        process_data_from_json(JSON_FILE)