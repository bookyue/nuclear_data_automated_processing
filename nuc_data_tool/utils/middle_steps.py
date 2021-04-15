from decimal import Decimal

from nuc_data_tool.utils.middle_steps_pb2 import MiddleStep, MiddleSteps


def serialization(middle_steps_list):
    middle_step = MiddleStep()
    middle_steps = MiddleSteps()
    for i, data in enumerate(middle_steps_list, start=1):
        middle_step.id = i
        middle_step.data = data
        middle_steps.middle_steps.append(middle_step)
    return middle_steps.SerializeToString()


def parsing(middle_steps_str):
    middle_steps = MiddleSteps()
    middle_steps.ParseFromString(middle_steps_str)
    return (middle_step for middle_step in middle_steps.middle_steps)


def middle_steps_line_serialization(data):
    """
    将middle_steps_line序列化
    len(data)小于10的话说明没有中间步骤，直接返回
    Parameters
    ----------
    data : list[str]

    Returns
    -------
    list[str]
    """
    if len(data) < 10:
        return data
    middle_steps = serialization(data[3: -1])
    return [*data[0:3], data[-1], middle_steps]


def middle_steps_line_parsing(data):
    """
    将middle_steps_line反序列化，并返回一个含序号和数据的字典

    Parameters
    ----------
    data : list[str]

    Returns
    -------
    dict
    """
    if data is None:
        return {'middle_steps': None}
    else:
        return {f'middle_step_{middle_step.id}': Decimal(middle_step.data) for middle_step in parsing(data)}
