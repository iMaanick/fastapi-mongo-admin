from collections.abc import Sequence

from starlette_admin import (
    BaseField,
    BooleanField,
    CollectionField,
    EnumField,
    FloatField,
    IntegerField,
    StringField,
    TagsField,
    TextAreaField,
)

from app.domain.course import Course, CourseLevel, CourseStatus
from app.presentation.admin.generic_mongo_view import GenericMongoView
from app.presentation.admin.lessons_list_field import LessonsListField


class MongoCourseView(GenericMongoView[Course]):
    """Админка для Course с автоматическим CRUD"""

    model_type = Course
    collection_name = "courses"
    database_name = "education_db"

    identity = "course"
    name = "Course"
    label = "Courses"
    icon = "fa fa-graduation-cap"
    pk_attr = "_id"

    fields: Sequence[BaseField] = [
        StringField(
            name="_id",
            exclude_from_create=True,
            exclude_from_edit=True,
            label="ID",
        ),
        StringField(
            name="title",
            required=True,
            label="Название курса",
            maxlength=200,
        ),
        TextAreaField(
            name="description",
            label="Описание",
            required=False,
        ),
        StringField(
            name="slug",
            required=True,
            label="URL slug",
            maxlength=100,
            placeholder="python-advanced-course",
        ),
        EnumField(
            name="status",
            enum=CourseStatus,
            label="Статус",
            required=True,
        ),
        EnumField(
            name="level",
            enum=CourseLevel,
            label="Уровень сложности",
            required=True,
        ),
        FloatField(
            name="price",
            label="Цена (₽)",
            required=True,
        ),
        StringField(
            name="instructor_name",
            label="Преподаватель",
            required=True,
            maxlength=150,
        ),
        BooleanField(
            name="is_published",
            label="Опубликован",
        ),

        TagsField(
            name="tags",
            label="Теги",
            help_text="Введите теги через запятую (например: python, backend, fastapi)",
        ),

        LessonsListField(
            field=CollectionField(
                name="lessons",
                fields=[
                    StringField(
                        name="title",
                        required=True,
                        label="Название урока",
                        maxlength=200,
                    ),
                    IntegerField(
                        name="duration_minutes",
                        required=True,
                        label="Длительность (мин)",
                    ),
                    IntegerField(
                        name="order",
                        required=True,
                        label="Порядковый номер",
                    ),
                    StringField(
                        name="video_url",
                        label="Ссылка на видео",
                        placeholder="https://youtube.com/watch?v=...",
                    ),
                    BooleanField(
                        name="is_free",
                        label="Бесплатный урок",
                    ),
                ],
            ),
        ),
    ]

    sortable_fields: tuple[str, ...] = (
        "_id",
        "title",
        "price",
        "status",
        "level",
        "is_published",
    )
    searchable_fields: Sequence[str] = ["title", "slug", "instructor_name"]
    exclude_fields_from_list: Sequence[str] = ["description", "lessons"]
    fields_default_sort: Sequence[str] = ["-_id"]
    page_size: int = 20
    page_size_options: Sequence[int] = [10, 20, 50, 100, -1]
