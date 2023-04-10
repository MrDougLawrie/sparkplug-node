## Import
# MQTT
import sparkplug_client as spc

# Simulation
import simulation

# Utility

## Initialise MQTT
BROKER_ADDRESS = 'localhost'
BROKER_PORT = 1883
KEEP_ALIVE_TIME = 60
BIRTH_CERTIFICATE_FILNAME = 'birth_certificate.json'

client = spc.Client(BROKER_ADDRESS, BROKER_PORT, KEEP_ALIVE_TIME)
client.set_birth_certificate(spc.BirthCertificate.from_file(BIRTH_CERTIFICATE_FILNAME))
client.connect()

## Initialise Simulation
params = {}
sim = simulation.WindFarm(params)


## Main Loop
while True:
    ## Check Connection
    if not client.connected:
        client.connect()

    ## Simulation - Calculate State
    sim_inputs = {}
    sim_outputs = sim.loop(sim_inputs)
    client.state.set

    ## Publish Changes
    client.publish_changes()
    
    ## Client Loop - await inputs
    # Result is a list of events such as NCMD, DCMD, Node Control
    result = client.inbound_events()
    
    ## Process inputs
    if result:
        continue