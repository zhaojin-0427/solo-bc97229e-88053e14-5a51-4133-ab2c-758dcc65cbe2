"""服务层异常，由路由层映射为 HTTP 响应。"""


class LineageError(Exception):
    """谱系服务业务异常基类。"""

    status_code = 400
    error_code = "lineage_error"

    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class CsvValidationError(LineageError):
    status_code = 422
    error_code = "csv_validation_error"


class CycleRejectedError(LineageError):
    """整批因合并后成环被拒绝（含自环）。"""

    status_code = 409
    error_code = "cycle_rejected"

    def __init__(self, cycle: list[str]):
        message = "批次被拒绝：合并后谱系存在环，已整批拒收"
        super().__init__(message, {"cycle": cycle})
        self.cycle = cycle


class UploadTooLargeError(LineageError):
    status_code = 413
    error_code = "upload_too_large"
