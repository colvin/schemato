/*
 * We decided we need more task states. Let's add them in version 2.
 */

INSERT INTO app.task_state
(state, descr)
VALUES
('ready', 'Task will be performed soon'),
('cancelled', 'Task will not be performed');
