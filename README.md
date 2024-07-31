# Fraud Detection
***
## Цели проектируемой антифрод-системы в соответствии с требованиями заказчика:
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
## Метрика машинного обучения:
Тк доля выявленных антифрод-системой мошеннических транзакций должна быть не меньше 98%, то для реализации этой цели нам нужно использовать метрику Recall.
Тк доля корректных транзакций, которые система определяет как мошеннические, не должна превышать 5%, то для реализации этой цели нам нужно использовать метрику Precision. 
Чтобы учесть обе вышеупомянутые метрики, нужно использовать метрику F-score.
Учитывая несбалансированность классов для анализа модели при различных пороговых значениях можем использовать PR кривую и соответствующую метрику PR-AUC.
***
## Анализ по MISSION Canvas:
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
  * Оценка и оптимизация затрат
* Развертывание модели
* Настройка мониторинга
***
## Настройка облачной инфраструктуры:
Данные в S3 доступны по адресу [https://storage.yandexcloud.net/ducket](https://storage.yandexcloud.net/ducket) или [s3://ducket/](s3://ducket/)

Список данных в HDFS:
![](hdfsscreen.jpeg)
### Оценка затрат:

* Стоимость часа аренды кластера без публичного доступа: **35,96 ₽/час**
  * Intel Ice Lake. 100% vCPU: 14,70 ₽
  * Yandex Data Proc - Intel Ice Lake. 100% vCPU: 1,68 ₽
  * Intel Ice Lake. RAM: 15,68 ₽
  * Yandex Data Proc - Intel Ice Lake. RAM: 1,68 ₽
  * Стандартное сетевое хранилище (HDD): 1,56 ₽
  * Быстрое сетевое хранилище (SSD): 0,66 ₽
* Cтоимость месяца аренды кластера без публичного доступа: 35,96 ₽ * 720 часов = **25891,2 ₽/месяц**
##### [Таблица тарифов на дисковые хранилища (SSD/HDD)](https://yandex.cloud/ru/docs/compute/pricing#prices-storage):

| Тип диска                      | Цена за 1 ГБ в месяц, ₽ |
|--------------------------------|----------------|
| Быстрый диск (SSD)             | 11,91          |
| Стандартный диск (HDD)         | 2,92           |
| Нереплицируемый диск (SSD)     | 8,80           |
| Высокопроизводительный диск (SSD) | 19,80       |

##### [Таблица тарифов на объектные хранилища (S3)](https://yandex.cloud/ru/docs/storage/pricing#prices-storage):

| Тип хранилища                  | Цена за 1 ГБ в месяц, ₽ |
|--------------------------------|----------------|
| Стандартное хранилище          | 2,01           |
| Холодное хранилище             | 1,07           |
| Ледяное хранилище              | 0,535          |
* Средняя цена 1 ГБ HDFS хранилища дороже средней цены 1 ГБ объектного в **9,0104** раза: 10,8575 ₽/месяц / 1,2050 ₽/месяц
  * Средняя цена 1 ГБ HDFS хранилища в месяц: (11,91 ₽/месяц + 2,92 ₽/месяц + 8,80 ₽/месяц + 19,80 ₽/месяц) / 4 =  10,8575  ₽/месяц
  * Средняя цена 1 ГБ S3 хранилища в месяц: (2,01 ₽/месяц + 1,07 ₽/месяц + 0,535 ₽/месяц) / 3 = 1,2050 ₽/месяц

## Оптимизация затрат:
* Использовать ледяное хранилище для данных с редким доступом
* Использовать HDD для данных с частым доступом
* Использовать нереплицируемый SSD, если возможно повторный скачивание и данные не имеют высокой ценности
* Снизить гарантированную долю vCPU до 20%
* Использовать прерывающиеся виртуальные машины
* Посмотреть список доступых льгот
***
## [Очистка данных](https://github.com/ilay-group/fraud_detection/blob/in-editing/notebook/Data%20cleaning.ipynb):
* Поиск проблем в данных:
  * Дубликаты
  * Пропуски
  * Отрицательные значения
  * Неправильный формат даты
  * Выбросы
* [Скрипт для очистки данных](https://github.com/ilay-group/fraud_detection/blob/in-editing/scripts/data_cleaning.py)
* Итоговые данные: [https://storage.yandexcloud.net/ducket4/](https://storage.yandexcloud.net/ducket4/) или [s3://ducket4/](s3://ducket4/)
## Автоматизация очистки данных:
* [DAG](https://github.com/ilay-group/fraud_detection/blob/main/dags/data_clear_dag.py)
  * [Скрипт для установки зависимостей](https://github.com/ilay-group/fraud_detection/blob/in-editing/scripts/install_env.py)
  * [Скрипт для очистки данных](https://github.com/ilay-group/fraud_detection/blob/in-editing/scripts/data_cleaning.py)
* Скриншот:![](screens/dag_screen.jpeg)
### ***
### В этом задании был использован airflow 3й версии т.к в нём была добавлена автоустановка зависимостей
## Обучение модели:
* Скрипт обучения модели: [train_model.py](https://github.com/ilay-group/fraud_detection/blob/in-editing/scripts/train_model.py)
* Скриншот:![](screens/mlflow_autotrain.jpeg)
## Автоматическое переобучение модели:
* DAG: [autotrain_model_dag.py](https://github.com/ilay-group/fraud_detection/blob/in-editing/dags/autotrain_model_dag.py)
* Скриншот:![](screens/airflow_autotrain.jpeg)
* bucket с артефактами: [https://storage.yandexcloud.net/fdghvjgfd](https://storage.yandexcloud.net/fdghvjgfd) или [s3://fdghvjgfd/](s3://fdghvjgfd/)
