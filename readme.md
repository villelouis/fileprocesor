### Задание
Написать программу на Python, которая делает следующие действия:
1. Создает 50 zip-архивов, в каждом 100 xml файлов со случайными данными следующей структуры:
```
<root>
    <var name=’id’ value=’<случайное уникальное строковое значение>’/>
    <var name=’level’ value=’<случайное число от 1 до 100>’/>
    <objects>
        <object name=’<случайное строковое значение>’/>
        <object name=’<случайное строковое значение>’/>
        …
    </objects>
</root>
```

В тэге objects случайное число (от 1 до 10) вложенных тэгов object.
2. Обрабатывает директорию с полученными zip архивами, разбирает вложенные xml файлы и формирует 2 csv файла:
Первый: id, level - по одной строке на каждый xml файл
Второй: id, object_name - по отдельной строке для каждого тэга object (получится от 1 до 10 строк на каждый xml файл)
Очень желательно сделать так, чтобы задание 2 эффективно использовало ресурсы многоядерного процессора.

### О результатах
#### Параметры системы
Запуск производился на СPython 3.9.6, на Apple M1 Max, 32GB RAM. Выполнение занимает ~ 0.5 сек.
#### Допущения
Сделал допущение, что сгенерированные данные можем писать целиком в память. 
Если считать, что в худшем варианте сгенерированная строка с xml занимает 0.7 Kb,
то в наихудшем сценарии в памяти будет: 0.7 Kb * 50 zip * 100 xml-файлов ~= 3.5 Mb

### Запуск
python3 main.py 
