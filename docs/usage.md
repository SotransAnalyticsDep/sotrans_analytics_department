# Руководство по использованию Sotrans Analytics Department

Данное руководство предназначено для пользователей и разработчиков, которым необходимо быстро разобраться в функциональности пакета. Здесь описаны примеры работы с данными, аналитикой, машинным обучением, оркестрацией процессов и дополнительными компонентами.

---

## 1. Введение

**Sotrans Analytics Department** — это комплексное решение для:
- Получения, очистки и трансформации данных.
- Анализа, моделирования и визуализации информации.
- Реализации алгоритмов машинного обучения и нейронных сетей.
- Оркестрации процессов и автоматизации CI/CD.
- Обеспечения безопасности, анонимизации и контроля целостности данных.

Это руководство поможет вам начать работу с пакетом, используя его основные модули и компоненты.

---

## 2. Установка и настройка

### 2.1 Установка пакета

1. **Клонируйте репозиторий:**

   ```bash
   git clone https://github.com/yourusername/source.git
   ```

2. **Перейдите в директорию проекта:**

   ```bash
   cd source
   ```

3. **Установите зависимости:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Установите пакет:**

   ```bash
   python setup.py install
   ```

### 2.2 Загрузка конфигурации

Все настройки централизованы в модуле `config`. Пример использования класса `Settings`:

```python
from source.config.settings import Settings

# Укажите путь к вашему файлу конфигурации (например, config.yaml или config.json)
settings = Settings("path/to/config.yaml")
config = settings.load()

# Получение конкретного параметра
db_conn = settings.get("database.connection_string")
print("Строка подключения к базе данных:", db_conn)
```

---

## 3. Работа с данными

### 3.1 Инжестия данных

Для загрузки данных из CSV файлов используется класс `CSVIngestor`.

```python
from source.data.ingestion.csv_ingestor import CSVIngestor

ingestor = CSVIngestor()
data = ingestor.ingest("data/sample_data.csv")
print("Загруженные данные:", data)
```

### 3.2 Очистка данных

Используйте класс `DataCleaner` для предварительной обработки и очистки данных.

```python
from source.data.cleaning.data_cleaner import DataCleaner

cleaner = DataCleaner()
cleaned_data = cleaner.clean(data)
print("Очищенные данные:", cleaned_data)
```

Также для обработки пропущенных значений примените `MissingValueHandler`:

```python
from source.data.cleaning.missing_handler import MissingValueHandler

missing_handler = MissingValueHandler()
imputed_data = missing_handler.impute(cleaned_data, strategy="mean")
print("Данные после обработки пропусков:", imputed_data)
```

### 3.3 Трансформация данных

Модуль `DataTransformer` отвечает за преобразование и агрегацию данных:

```python
from source.data.transformation.transformer import DataTransformer

transformer = DataTransformer()
transformed_data = transformer.transform(imputed_data)
print("Преобразованные данные:", transformed_data)
```

### 3.4 Хранение данных

Для работы с базой данных используйте `DatabaseConnector`:

```python
from source.data.storage.database_connector import DatabaseConnector

db = DatabaseConnector()
connection = db.connect("sqlite:///my_database.db")
result = db.execute_query("SELECT * FROM my_table")
print("Результат запроса:", result)
db.disconnect()
```

---

## 4. Инженерия процессов

### 4.1 Создание конвейера обработки

С помощью `PipelineManager` можно объединить несколько этапов обработки в единый рабочий поток:

```python
from source.engineering.pipeline.pipeline_manager import PipelineManager

# Допустим, ваши этапы: инжестия, очистка и трансформация
stages = [ingestor, cleaner, transformer]
pipeline = PipelineManager().create_pipeline(stages)

final_data = pipeline.execute_pipeline("data/sample_data.csv")
print("Данные после прохождения конвейера:", final_data)
```

### 4.2 Оркестрация задач

Класс `Orchestrator` позволяет планировать выполнение задач через определённые интервалы времени.

```python
from source.engineering.orchestration.orchestrator import Orchestrator
import time

def sample_task():
    print("Выполняется запланированная задача...")

orchestrator = Orchestrator()
orchestrator.schedule(sample_task, interval=60)  # Запуск задачи каждую минуту

# Для демонстрации можно запустить оркестратор (будет выполняться бесконечно)
# orchestrator.run()
```

---

## 5. Аналитика и моделирование

### 5.1 Статистический анализ

Используйте `StatsCalculator` для вычисления основных статистических показателей:

```python
from source.analytics.statistical.statistics.stats_calculator import StatsCalculator

stats_calc = StatsCalculator()
mean_value = stats_calc.calculate_mean([1, 2, 3, 4, 5])
print("Среднее значение:", mean_value)
```

### 5.2 Моделирование данных

#### Регрессионная модель

```python
from source.analytics.statistical.modeling.regression import RegressionModel

reg_model = RegressionModel()
reg_model.train(X_train, y_train)
predictions = reg_model.predict(X_test)
print("Прогноз регрессионной модели:", predictions)
```

#### Классификационная модель

```python
from source.analytics.statistical.modeling.classification import ClassificationModel

cls_model = ClassificationModel()
cls_model.train(X_train, y_train)
predictions = cls_model.predict(X_test)
print("Прогноз классификационной модели:", predictions)
```

#### Оценка моделей

```python
from source.analytics.statistical.evaluation.evaluator import Evaluator

evaluator = Evaluator()
metrics = evaluator.evaluate_model(reg_model, test_data)
print("Метрики модели:", metrics)
```

---

## 6. Машинное обучение и нейронные сети

### 6.1 Традиционные алгоритмы машинного обучения

Пример использования `RandomForest` и обучения модели:

```python
from source.ml.traditional.algorithms.random_forest import RandomForest
from source.ml.traditional.training.trainer import MLTrainer

rf_model = RandomForest()
trainer = MLTrainer()
trainer.train_model(rf_model, X_train, y_train)
predictions = rf_model.predict(X_test)
print("Прогноз RandomForest:", predictions)
```

### 6.2 Нейронные сети

Пример создания и обучения модели CNN:

```python
from source.ml.deep_learning.architectures.cnn import CNNModel
from source.ml.deep_learning.training.nn_trainer import NNTrainer

cnn = CNNModel()
model = cnn.build_model(input_shape=(28, 28, 1), num_classes=10)
cnn.compile_model(optimizer="adam", loss="categorical_crossentropy")

nn_trainer = NNTrainer()
nn_trainer.train(model, X_train, y_train, epochs=10)
results = nn_trainer.evaluate(model, X_test)
print("Результаты обучения CNN:", results)
```

---

## 7. Оркестрация задач

### 7.1 Планировщик задач

Для сложных сценариев использования применяйте `TaskScheduler`:

```python
from source.orchestration.scheduler.task_scheduler import TaskScheduler

scheduler = TaskScheduler()
scheduler.schedule_task(sample_task, "0 0 * * *")  # Запуск задачи каждый день в полночь
```

### 7.2 Управление рабочими процессами

Организуйте сложные ETL-конвейеры с помощью `WorkflowManager`:

```python
from source.orchestration.workflow.workflow_manager import WorkflowManager

def another_task():
    print("Выполнение второй задачи")

workflow_manager = WorkflowManager()
workflow = workflow_manager.create_workflow([sample_task, another_task])
workflow_manager.execute_workflow(workflow)
```

---

## 8. Дополнительные компоненты

### 8.1 Распределённая обработка

Используйте `SparkConnector` для интеграции с Apache Spark:

```python
from source.additional_components.distributed_processing.spark_connector import SparkConnector

spark_connector = SparkConnector()
spark_session = spark_connector.connect("spark://master:7077")
result = spark_connector.run_job(lambda spark: spark.sql("SELECT * FROM my_table"))
print("Результат Spark-запроса:", result)
spark_connector.disconnect()
```

### 8.2 Обработка потоковых данных

Подключение к источнику потоковых данных (например, Apache Kafka) с использованием `StreamProcessor`:

```python
from source.additional_components.real_time_analytics.stream_processor import StreamProcessor

stream_processor = StreamProcessor()
stream_processor.connect_to_source("kafka://broker:9092")
processed_stream = stream_processor.process_stream("stream_topic")
print("Обработанный поток данных:", processed_stream)
stream_processor.disconnect()
```

### 8.3 Анонимизация данных

Пример использования `DataAnonymizer` для защиты конфиденциальной информации:

```python
from source.additional_components.data_anonymization.anonymizer import DataAnonymizer

anonymizer = DataAnonymizer()
anonymized_data = anonymizer.anonymize(data)
print("Анонимизированные данные:", anonymized_data)
```

---

## 9. Тестирование

Для запуска тестов используйте команду:

```bash
pytest
```

Каталог `tests/` содержит примеры юнит-тестов для основных компонентов. Рекомендуется регулярно запускать тесты при внесении изменений в код.

---

## 10. Заключение

Пакет **Sotrans Analytics Department** предоставляет широкие возможности для обработки, анализа, моделирования и визуализации данных, а также для реализации процессов машинного обучения и оркестрации задач. Надеемся, что данное руководство поможет вам быстро освоить основные функции пакета и адаптировать их под задачи вашего проекта.

Если у вас возникнут вопросы или предложения, пожалуйста, создавайте Issues на [GitHub](https://github.com/SotransAnalyticsDep/sotrans_analytics_department/issues).

---

*Примечание:* Приведённые примеры кода являются демонстрационными. Перед использованием в продакшене убедитесь в корректности и соответствии вашим требованиям.
```

Данный файл `docs/usage.md` поможет вам и вашим коллегам быстро понять, как использовать ключевые возможности проекта «Sotrans Analytics Department», а также послужит отправной точкой для написания собственных примеров и адаптации функционала под конкретные задачи.
