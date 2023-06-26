import time
import random
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from benchmark import timing


@timing
def get_current_dislocation() -> List:
    """
    Формирование текущей дислокации вагонов.
    Получаем список вагонов и их дату прибытия.
    Каждый вагон может быть привязан к одной и той же накладной!
    Для того, чтобы получить предсказанную дату прибытия, необходимо вызывать сервис 'get_predicted_dates'
    """
    locations = []
    arrivale_dates = [None, None, None, datetime.now() - timedelta(days=3), datetime.now()]
    time.sleep(2)

    for i in range(0, 20000):
        arrivale_date = random.choice(arrivale_dates)
        location = {
            "wagon": random.randint(10000, 90000),
            "invoice": f"{random.randint(1, 30000)}__HASH__",
            "arrivale_date": arrivale_date.strftime("%d.%m.%Y") if arrivale_date else None,
        }
        locations.append(location)
    return locations


@timing
def get_predicted_date_by_invoices(invoices: List) -> List:
    """
    На вход необходимо передать список из уникальных накладных.
    По каждой накладной будет сформировано время прибытия
    """
    time.sleep(1)
    predicted_results = []
    for invoice in invoices:
        predicted_date = datetime.now() + timedelta(days=random.randint(1, 5))
        data = {
            "invoice": invoice,
            "predicted_date": predicted_date.strftime("%d.%m.%Y")
        }
        predicted_results.append(data)

    return predicted_results


def add_predicted_date(inv_no_date: List, inv_dict: Dict, predicted_date: Dict) -> None:
    """
   Метод выставляет предсказанную дату к накладным у которых отсутствует дата.
   На вход передается:
    1. Список из накладных у которых нет даты
    2. Словарь для хранения накладных и всех вагонов, привязанных к ней
    3. Предсказанная дата из сервиса get_predicted_date_by_invoices
    """
    for inv in inv_no_date:
        wagons = inv_dict.get(inv)
        if wagons:
            date = predicted_date.get(inv, None)
            for wagon in wagons:
                wagon['arrivale_date'] = date


def processing_invoice(dis: List[Dict]) -> Tuple[Dict, List]:
    """
   Метод добавляет к накладной все вагоны, которые с ней связаны.
   И отбирает накладные без даты прибытия.
   На вход передается список всех вагонов с накладными из сервиса get_current_dislocation.
    """

    # Словарь для хранения накладных и всех вагонов, привязанных к ней.
    # Учитываем, что каждый вагон может быть привязан к одной и той же накладной!
    invoice_dict = {}

    # Список накладных без указанной даты прибытия
    invoice_list_no_date = []

    for wagon in dis:
        invoice = wagon['invoice']
        if invoice not in invoice_dict:
            invoice_dict[invoice] = []
        invoice_dict[invoice].append(wagon)

        if wagon['arrivale_date'] is None:
            invoice_list_no_date.append(invoice)

    return invoice_dict, invoice_list_no_date


@timing
def api_call():
    """
    В качестве ответа должен выдаваться повагонный список из сервиса get_current_dislocation
    с обновленной датой прибытия вагона из сервиса get_predicted_dates
    только по вагоном, у которых она отсутствует
    """

    retries = 3
    delay = 2

    for attempt in range(1, retries + 1):
        try:
            dislocation = get_current_dislocation()

            invoice_dict, invoice_list_no_date = processing_invoice(dislocation)

            unique_invoices = list(set(invoice_list_no_date))

            predict_date = get_predicted_date_by_invoices(unique_invoices)
            predict_date_dict = {d['invoice']: d['predicted_date'] for d in predict_date}

            add_predicted_date(invoice_list_no_date, invoice_dict, predict_date_dict)

            return dislocation

        except Exception as e:
            print(f"Лог: {e}")

            if attempt < retries:
                print(f"Повторная попытка через {delay} секунды")
                time.sleep(delay)

    raise ConnectionError("Ошибка сервера")
