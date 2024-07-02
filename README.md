# Fraud Detection
***
## Цели проектируемой антифрод-системы в соответствии с требованиями заказчика.
* Бюджет создания антифрод-системы не должен превысить 10 млн. руб. (не считая зарплат специалистам)
* MVP должно быть готово через три месяца
* Антифрод-система должна быть готова через пол года
* Доля выявленных антифрод-системой мошеннических транзакций должна быть не меньше 98%
* Общий ущерб клиентов за месяц не должен превышать 500 тыс. руб.
* Система должна быть способна выдержать пиковые нагрузки на 700% превышающие норму
* Доля корректных транзакций, которые система определяет как мошеннические, не должна превышать 5%
* Система должна быть развернута на облачных вычислительных ресурсах
* Данные о клиентах должны быть обезличены
***
## Метрика машинного обучения.
Тк доля выявленных антифрод-системой мошеннических транзакций должна быть не меньше 98%, то для реализации этой цели нам нужно использовать метрику Recall.
Тк доля корректных транзакций, которые система определяет как мошеннические, не должна превышать 5%, то для реализации этой цели нам нужно использовать метрику Precision. 
Чтобы учесть обе вышеупомянутые метрики, нужно использовать метрику F-score.
Учитывая несбалансированность классов для анализа модели при различных пороговых значениях можем использовать PR кривую и соответствующую метрику PR-AUC.
***
## Анализ по MISSION Canvas.
* Partnership: облачные сервисы
* Activities: аренда облачных сервисов, разработка модели
* Resources: кластеры серверов, транзакции клиентов
* Value propositions: повышенная безопасность проведения операций
* Buy-in & Support: аналитики данных, бизнес-аналитики, инженеры, разработчики
* Deployment: Коннектор; обработка/инференс; СХД; мониторинг. + CI/CD
* Beneficiaries: граждане, пользующиеся услугами проведения онлайн-платежей с банковских счетов
* Budget / cost: бюджет не должен превысить 10 млн. руб. (не считая зарплат специалистам)
* Achievements / impact factors: доля выявленных ошеннических транзакций должна быть не меньше 98% + доля корректных транзакций, которые система определяет как мошеннические, не должна превышать 5% + общий ущерб клиентов за месяц не должен превышать 500 тыс. руб.
***
## Задачи:
* Подготовка данных
  * Очистка данных:
    * Поиск проблем в данных
    * Написание скрипта для очистки данных
* Разработка модели
* Настройка облачной инфраструктуры
  * Создание S3 хранилища  
  * Копирование данных с S3 компании на свой S3
  * Копирование данных с S3 в HDFS
* Развертывание модели
* Настройка мониторинга
***
## Настройка облачной инфраструктуры:
Данные в S3 доступны по адресу [https://storage.yandexcloud.net/ducket](https://storage.yandexcloud.net/ducket)

Список данных в HDFS:
![](hdfsscreen.jpeg)
### Оценка затрат
##### [Таблица тарифов на дисковые хранилища (SSD/HDD)](https://yandex.cloud/ru/docs/compute/pricing#prices-storage)

| Тип диска                      | Цена за 1 ГБ, ₽ |
|--------------------------------|----------------|
| Быстрый диск (SSD)             | 11,91          |
| Стандартный диск (HDD)         | 2,92           |
| Нереплицируемый диск (SSD)     | 8,80           |
| Высокопроизводительный диск (SSD) | 19,80       |

##### [Таблица тарифов на объектные хранилища (S3)](https://yandex.cloud/ru/docs/storage/pricing#prices-storage)

| Тип хранилища                  | Цена за 1 ГБ, ₽ |
|--------------------------------|----------------|
| Стандартное хранилище          | 2,01           |
| Холодное хранилище             | 1,07           |
| Ледяное хранилище              | 0,535          |

* Общая стоимость кластера без публичного доступа: **25891,2 ₽/месяц**
* Средняя цена HDFS-хранилища дороже средней цены объектного в **9,0104** раза

## Оптимизация затрат
* Использовать ледяное хранилище для данных с редким доступом
* Использовать HDD для данных с частым доступом
* Использовать нереплицируемый SSD, если возможно повторный скачивание и данные не имеют высокой ценности
* Снизить гарантированную долю vCPU до 20%
* Использовать прерывающиеся виртуальные машины
* Посмотреть список доступых льгот
***
## [Очистка данных](https://github.com/ilay-group/fraud_detection/blob/in-editing/notebook/Data%20cleaning.ipynb):
* [Поиск проблем в данных](https://i.ytimg.com/vi/L9W4oeEwUSY/maxresdefault.jpg):
  * Дубликаты
  * Выбросы
  * Отрицательные значения
  * Пропуски
  * Неправильный формат даты
* [Скрипт для очистки данных](https://github.com/ilay-group/fraud_detection/blob/in-editing/scripts/data_cleaning.py)
* Итоговые данные: [https://storage.yandexcloud.net/ducket4/](https://storage.yandexcloud.net/ducket4/)
