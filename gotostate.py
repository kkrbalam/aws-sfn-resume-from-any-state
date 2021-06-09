import argparse
import json
import time
from datetime import datetime
import boto3

client = boto3.client('stepfunctions', region_name="us-east-1")

class StateMachineFailedError(Exception):
    """Base class for other exceptions"""
    pass

def smArnFromExecutionArn(arn):
    '''
    Get the State Machine Arn from the execution Arn
    Input: Execution Arn of a state machine
    Output: Arn of the state machine
    '''
    smArn = arn.split(':')[:-1]
    smArn[5] = 'stateMachine'
    return ':'.join(smArn)


def parseFailureHistory(failedExecutionArn):
    '''
    Parses the execution history of a failed state machine to get the name of failed state and
    the input to the failed state
    Input failedExecutionArn - a string containing the execution Arn of a failed state machine
    Output - a list with two elements: [name of failed state, input to failed state]
    '''

    failedEvents = list()
    failedAtParallelState = False

    try:
        # Get the execution history
        response = client.get_execution_history(
            executionArn=failedExecutionArn,
            reverseOrder=True
        )
        next_token = response.get('nextToken')
        failedEvents.extend(response['events'])
    except Exception as ex:
        raise ex

    while next_token is not None:
        try:
            # Get the execution history
            response = client.get_execution_history(
                executionArn=failedExecutionArn,
                reverseOrder=True,
                nextToken=next_token
            )
            next_token = response.get('nextToken')
            failedEvents.extend(response['events'])
        except Exception as ex:
            raise ex

    # Confirm that the execution actually failed, raise exception if it didn't fail
    try:
        failedEvents[0]['executionFailedEventDetails']
    except:
        raise ('Execution did not fail')
    '''
    If we have a 'States.Runtime' error (for example if a task state in our state 
    machine attempts to execute a lambda function in a different region than the 
    state machine, get the id of the failed state, use id of the failed state to
    determine failed state name and input
    '''
    if failedEvents[0]['executionFailedEventDetails']['error'] == 'States.Runtime':
        failedId = int(filter(str.isdigit, str(failedEvents[0]['executionFailedEventDetails']['cause'].split()[13])))
        failedState = failedEvents[-1 * failedId]['stateEnteredEventDetails']['name']
        failedInput = failedEvents[-1 * failedId]['stateEnteredEventDetails']['input']
        return (failedState, failedInput)
    '''
    We need to loop through the execution history, tracing back the executed steps
    The first state we encounter will be the failed state
    If we failed on a parallel state, we need the name of the parallel state rather than the 
    name of a state within a parallel state it failed on. This is because we can only attach
    the goToState to the parallel state, but not a sub-state within the parallel state.
    This loop starts with the id of the latest event and uses the previous event id's to trace
    back the execution to the beginning (id 0). However, it will return as soon it finds the name
    of the failed state 
    '''
    currentEventId = failedEvents[0]['id']
    while currentEventId != 0:
        # multiply event id by -1 for indexing because we're looking at the reversed history
        currentEvent = failedEvents[-1 * currentEventId]
        '''
        We can determine if the failed state was a parallel state because it an event
        with 'type'='ParallelStateFailed' will appear in the execution history before
        the name of the failed state
        '''
        if currentEvent['type'] == 'ParallelStateFailed':
            failedAtParallelState = True
        '''
        If the failed state is not a parallel state, then the name of failed state to return
        will be the name of the state in the first 'TaskStateEntered' event type we run into 
        when tracing back the execution history
        '''
        if currentEvent['type'] == 'TaskStateEntered' and failedAtParallelState == False:
            failedState = currentEvent['stateEnteredEventDetails']['name']
            failedInput = currentEvent['stateEnteredEventDetails']['input']
            return (failedState, failedInput)
        '''
        If the failed state was a paralell state, then we need to trace execution back to 
        the first event with 'type'='ParallelStateEntered', and return the name of the state
        '''
        if currentEvent['type'] == 'ParallelStateEntered' and failedAtParallelState:
            failedState = currentEvent['stateEnteredEventDetails']['name']
            failedInput = currentEvent['stateEnteredEventDetails']['input']
            return (failedState, failedInput)
        # Update the id for the next execution of the loop
        currentEventId = currentEvent['previousEventId']


def attachGoToState(failedStateName, stateMachineArn, failedStateMachineName):
    '''
    Given a state machine arn and the name of a state in that state machine, create a new state machine
    that starts at a new choice state called the 'GoToState'. The "GoToState" will branch to the named
    state, and send the input of the state machine to that state, when a variable called "resuming" is
    set to True
    Input   failedStateName - string with the name of the failed state
            stateMachineArn - string with the Arn of the state machine
    Output  response from the create_state_machine call, which is the API call that creates a new state machine
    '''
    try:
        response = client.describe_state_machine(
            stateMachineArn=stateMachineArn
        )
    except:
        raise ('Could not get ASL definition of state machine')
    roleArn = response['roleArn']
    stateMachine = json.loads(response['definition'])
    # Create a name for the new state machine
    newName = failedStateMachineName + '-FR-' + datetime.now().strftime("%Y%m%d")
    # Get the StartAt state for the original state machine, because we will point the 'GoToState' to this state
    originalStartAt = stateMachine['StartAt']
    '''
    Create the GoToState with the variable $.resuming
    If new state machine is executed with $.resuming = True, then the state machine will skip to the failed state
    Otherwise, it will execute the state machine from the original start state
    '''
    goToState = {'Type': 'Choice',
                 'Choices': [{'Variable': '$.resuming', 'BooleanEquals': False, 'Next': originalStartAt}],
                 'Default': failedStateName}
    # Add GoToState to the set of states in the new state machine
    stateMachine['States']['GoToState'] = goToState
    # Add StartAt
    stateMachine['StartAt'] = 'GoToState'
    # Create new state machine
    try:
        response = client.create_state_machine(
            name=newName,
            definition=json.dumps(stateMachine),
            roleArn=roleArn
        )
    except:
        raise ('Failed to create new state machine with GoToState')
    return response


def executeRerunStateMachineWithFailedInput(smArn: str, failedInput: str):
    """
    This function will execute the newly created rerun state machine with failed input data
    Remember to add resuming=True to failed input
    Args:
    smArn: arn of the rerun statemachine
    failedInput: string version of FailedInput
    """

    try:
        failedInputDict = json.loads(failedInput)
        failedInputDict["resuming"] = True
        response = client.start_execution(
            stateMachineArn=smArn,
            name=smArn.split(":")[-1] + "-" + datetime.now().strftime("%H%M%S"),
            input=json.dumps(failedInputDict)
        )
        return response
    except Exception as e:
        raise (f"Exception occurred trying to trigger state machine with arn: {smArn}, error is {e}")


def deleteRerunStateMachine(smArn: str):
    """
    Delete the state machine after execution
    smArn: Rerun state machine arn
    """
    try:
        client.delete_state_machine(
            stateMachineArn=smArn
        )
    except:
        raise ('Failed to delete state machine with GoToState')


def getStateMachineOutput(executionArn: str) -> dict:
    """
    Checks running state machine's status and if successful returns output
    :param region: region where step function is executing
    :param execution_arn: execution arn of the state machine
    """
    try:
        status = 'RUNNING'
        response = {}
        while status == 'RUNNING':
            response = client.describe_execution(executionArn=executionArn)
            status = response["status"]
            if status == 'RUNNING':
                time.sleep(10)
                print("State Machine is still running, sleeping for 10 secs")   
        if status != "SUCCEEDED":
            print(status)
            raise StateMachineFailedError(f"step function execution for arn {executionArn} {status}")
        return response

    except Exception as e:
        raise (f"Failed to get status of step function, error is {e}")


if __name__ == '__main__':
    '''
    Main
    Run as: 
    python gotostate.py --failedExecutionArn '<Failed_Execution_Arn>'"
    '''
    parser = argparse.ArgumentParser(description='Execution Arn of the failed state machine.')
    parser.add_argument('--failedExecutionArn', dest='failedExecutionArn', type=str)
    args = parser.parse_args()
    failedSMInfo = parseFailureHistory(args.failedExecutionArn)
    smArn = smArnFromExecutionArn(args.failedExecutionArn)
    failedExecutionName = args.failedExecutionArn.split(":")[-1]
    newMachine_response = attachGoToState(failedSMInfo[0], smArn, failedExecutionName)

    print("New State Machine Arn: {}".format(newMachine_response['stateMachineArn']))
    print("Execution had failed at state: {} with Input: {}".format(failedSMInfo[0], failedSMInfo[1]))

    print("------------------------------------------------------------------------------------")
    print("-------------------- Now Executing SM from failed state ----------------------------")
    print("------------------------------------------------------------------------------------")

    # Here you will call the functions to execute the state machine
    try:
        executionResponse = executeRerunStateMachineWithFailedInput(newMachine_response['stateMachineArn'], failedSMInfo[1])
        executionArn = executionResponse["executionArn"]
        statusResponse = getStateMachineOutput(executionArn)
        print("Delete new rerun workflow")
        deleteRerunStateMachine(newMachine_response['stateMachineArn'])
    except:
        print("Step Function execution failed new rerun workflow!!!!")
