from utils.middle_steps_pb2 import MiddleStep, MiddleSteps


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


def middle_steps_serialization(physical_quantity, data):
    if len(data) < 10:
        return data
    if physical_quantity != 'gamma_spectra':
        start = 3
    else:
        start = 2
    middle_steps = serialization(data[start: -1])
    return [*data[0:start], data[-1], middle_steps]


def middle_steps_parsing(data):
    if data is None:
        return {'middle_steps': None}
    else:
        return {f'middle_step_{middle_step.id}': middle_step.data for middle_step in parsing(data)}
