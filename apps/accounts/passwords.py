"""Password policy for staff accounts provisioned by station administrators."""

from django.contrib.auth.password_validation import (
    MinimumLengthValidator,
    UserAttributeSimilarityValidator,
    get_default_password_validators,
    validate_password,
)


STAFF_PASSWORD_MIN_LENGTH = 5


def validate_staff_password(password, user=None):
    validators = [
        MinimumLengthValidator(STAFF_PASSWORD_MIN_LENGTH)
        if type(validator) is MinimumLengthValidator
        else validator
        for validator in get_default_password_validators()
        if not isinstance(validator, UserAttributeSimilarityValidator)
    ]
    validate_password(password, user, password_validators=validators)
