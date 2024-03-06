CREATE table if not exists  n_node (
    nn_id SERIAL PRIMARY KEY,
    nn_name varchar(255),
    nn_child_count INTEGER
);

CREATE table if not exists  n_relation (
    nn_parent_id INTEGER REFERENCES n_node(nn_id),
    nn_children_id INTEGER REFERENCES n_node(nn_id)
);