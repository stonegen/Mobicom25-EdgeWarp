## Source code for EdgeCatalyst project.

This code repo contains all the source code used in the evaluation of "A Cross-Layer Design For Enabling Stateful Mobile
Edge Applications Over 5G" paper.


1. **app_aware_5g_control_plane** folder contains source code related to the application-aware cellular control plane.
2. **carmap_with_edgecat** folder contains the CarMap application code modified with EdgeCat.
3. **emp_with_edgecat** folder contains the EMP application code modified with EdgeCat.
4. **redis-unstable** this contains the redis source code along with the modifications we did for the asynchronous migration. Please see the readMe file (**redis-unstable/README.md**) of the redis-unstable for the detailed overview of our modifications.
5. **state_migration_simulation_framework** folder contains the code used to simulate applications having diverse state properties. Please see the readMe file (**state_migration_simulation_framework/README.md**) of the state_migration_simulation_framework for the detailed overview of the simulation.
6. **target_base_station_prediction** folder contains the code related to handover prediction and base station prediction and pre-processing of the datasets used to evaluate the target BS prediction pipeline. The datasets include radio traces collected in high-speed trains, driving tests, and other miscellaneous mobility scenarios.

*****Further Detailed documentation will be available post-publication.*****
