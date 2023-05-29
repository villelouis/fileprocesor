import abc
import xml.etree.ElementTree as ET

from src import errors, model, utils


class Converter(abc.ABC):
    EXTENSION = 'unknown'

    @abc.abstractmethod
    def dumps(self, root_dto: model.RootDTO) -> str:
        """
        :return: prepare string presentation of RootDTO for send or write to file in specific manner
        """
        pass

    @abc.abstractmethod
    def loads(self, str_presentation: str) -> model.RootDTO:
        """
        :return:
        """
        pass


class XMLConverter(Converter):
    EXTENSION = 'xml'

    def dumps(self, root_dto: model.RootDTO) -> str:
        """
            Сделал допущение, что сгенерированные данные можем писать целиком в память.
            Если считать, что в худшем варианте файл в данные по 1 генерации занимают 0.7 Kb,
            то в наихудшем сценарии в памяти будет: 0.7 Kb * 50 zip * 100 xml-файлов ~= 3.5 Mb

            Получаем представление:
                <root>
                    <var name=’id’ value=’<случайное уникальное строковое значение>’/>
                    <var name=’level’ value=’<случайное число от 1 до 100>’/>
                    <objects>
                        <object name=’<случайное строковое значение>’/>
                        <object name=’<случайное строковое значение>’/>
                        …
                    </objects>
                </root>
        """
        root = ET.Element('root')
        root.append(ET.Element('id', attrib={'value': root_dto.id}))
        root.append(ET.Element('level', attrib={'value': str(root_dto.level)}))
        objects = ET.SubElement(root, 'objects')
        for object_dto in root_dto.objects:
            ET.SubElement(objects, 'object', attrib={'name': object_dto.name})
        return ET.tostring(root, encoding='unicode', method='xml')

    def loads(self, str_presentation: str) -> model.RootDTO:
        root = ET.fromstring(str_presentation)
        _id, level, objects = None, None, []
        for child in root:
            if child.tag == 'id':
                _id = child.attrib.get('value')
            if child.tag == 'level':
                level = child.attrib.get('value')
            if child.tag == 'objects':
                for obj in child:
                    name = obj.attrib.get('name')
                    if not name:
                        raise errors.InputValidationError('wrong input XML-str: name is missing for object')
                    objects.append(model.ObjectDTO(name=name))

        if not _id or not level or not objects:
            raise errors.InputValidationError(
                f'wrong input XML-str: _id={_id} level={level} objects={objects} fields must not be empty')

        return model.RootDTO(
            id=_id,
            level=int(level),
            objects=objects
        )


if __name__ == '__main__':
    with open('sample.xml', 'w') as f:
        f.write(XMLConverter().dumps(utils.get_random_object_dto()))

    with open('sample.xml', 'r') as f:
        res = f.read()
        print(XMLConverter().loads(res))
