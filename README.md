# Fraud Detection

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

## Метрика машинного обучения.
Тк доля выявленных антифрод-системой мошеннических транзакций должна быть не меньше 98%, то для реализации этой цели нам нужно использовать метрику Recall.
Тк доля корректных транзакций, которые система определяет как мошеннические, не должна превышать 5%, то для реализации этой цели нам нужно использовать метрику Precision. 
Чтобы учесть обе вышеупомянутые метрики, нужно использовать метрику F-score.
Учитывая несбалансированность классов для анализа модели при различных пороговых значениях можем использовать PR кривую и соответствующую метрику PR-AUC.


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

## Задачи:
* Подготовка данных
* Разработка модели
* Настройка облачной инфраструктуры
* Развертывание модели
* Настройка мониторинга
