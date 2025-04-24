from datetime import datetime


class DataRow:

    def __init__(self, event_app: str, event: str, event_date: int, event_time: int, event_id: int, string_00: str,
                 string_01: str, string_02: str, string_03: str, string_04: str, string_05: str, string_06: str,
                 string_07: str, string_08: str, string_09: str, string_10: str, string_11: str, string_12: str,
                 string_13: str, string_14: str, string_15: str, string_16: str, string_17: str, string_18: str,
                 string_19: str, float_00: float, float_01: float, float_02: float, float_03: float, float_04: float,
                 float_05: float, float_06: float, float_07: float, float_08: float, float_09: float, float_10: float,
                 float_11: float, float_12: float, float_13: float, float_14: float, float_15: float, float_16: float,
                 float_17: float, float_18: float, float_19: float, string_20: str, string_21: str, string_22: str,
                 string_23: str, string_24: str, string_25: str, string_26: str, string_27: str, string_28: str,
                 string_29: str, float_20: float, float_21: float, float_22: float, float_23: float, float_24: float,
                 float_25: float, float_26: float, float_27: float, float_28: float, float_29: float,
                 event_context: str, event_tick: int):
        self.event_app: str = event_app
        self.event: str = event
        self.event_date: str = datetime.utcfromtimestamp(event_date).strftime('%Y-%m-%d %H:%M:%S')
        self.event_time: int = event_time
        self.event_id: int = event_id
        self.string_00: str = string_00
        self.string_01: str = string_01
        self.string_02: str = string_02
        self.string_03: str = string_03
        self.string_04: str = string_04
        self.string_05: str = string_05
        self.string_06: str = string_06
        self.string_07: str = string_07
        self.string_08: str = string_08
        self.string_09: str = string_09
        self.string_10: str = string_10
        self.string_11: str = string_11
        self.string_12: str = string_12
        self.string_13: str = string_13
        self.string_14: str = string_14
        self.string_15: str = string_15
        self.string_16: str = string_16
        self.string_17: str = string_17
        self.string_18: str = string_18
        self.string_19: str = string_19
        self.float_00: float = float_00
        self.float_01: float = float_01
        self.float_02: float = float_02
        self.float_03: float = float_03
        self.float_04: float = float_04
        self.float_05: float = float_05
        self.float_06: float = float_06
        self.float_07: float = float_07
        self.float_08: float = float_08
        self.float_09: float = float_09
        self.float_10: float = float_10
        self.float_11: float = float_11
        self.float_12: float = float_12
        self.float_13: float = float_13
        self.float_14: float = float_14
        self.float_15: float = float_15
        self.float_16: float = float_16
        self.float_17: float = float_17
        self.float_18: float = float_18
        self.float_19: float = float_19
        self.string_20: str = string_20
        self.string_21: str = string_21
        self.string_22: str = string_22
        self.string_23: str = string_23
        self.string_24: str = string_24
        self.string_25: str = string_25
        self.string_26: str = string_26
        self.string_27: str = string_27
        self.string_28: str = string_28
        self.string_29: str = string_29
        self.float_20: float = float_20
        self.float_21: float = float_21
        self.float_22: float = float_22
        self.float_23: float = float_23
        self.float_24: float = float_24
        self.float_25: float = float_25
        self.float_26: float = float_26
        self.float_27: float = float_27
        self.float_28: float = float_28
        self.float_29: float = float_29
        self.event_context: str = event_context
        self.event_tick: int = event_tick

    @classmethod
    def from_tuple(cls, data):
        return cls(*data)


testRow = DataRow(
    "event_app_value",
    "game_loading_error",
    1418188296,
    1418188296,
    402752555,
    "string_00",
    "string_01",
    "string_02",
    "string_03",
    "string_04",
    "string_05",
    "string_06",
    "string_07",
    "string_08",
    "string_09",
    "string_10",
    "string_11",
    "string_12",
    "string_13",
    "string_14",
    "string_15",
    "string_16",
    "string_17",
    "string_18",
    "string_19",
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    "string_20",
    "string_21",
    "string_22",
    "string_23",
    "string_24",
    "string_25",
    "string_26",
    "string_27",
    "string_28",
    "string_29",
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    "event_context",
    10
)
