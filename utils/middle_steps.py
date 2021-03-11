from utils.middle_steps_pb2 import MiddleStep, MiddleSteps


def serialization(middle_steps_list):
    middle_step = MiddleStep()
    middle_steps = MiddleSteps()
    for i, data in enumerate(middle_steps_list):
        middle_step.id = i
        middle_step.data = data
        middle_steps.middle_steps.append(middle_step)
    return middle_steps.SerializeToString()
