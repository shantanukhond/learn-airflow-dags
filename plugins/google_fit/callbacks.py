def alert_on_failure(context) -> None:
    """--on_failure_callback: runs when a task fails."""
    ti = context["task_instance"]
    print(
        f"[on_failure_callback] task={ti.task_id} "
        f"dag={ti.dag_id} run_id={context.get('run_id')}"
    )


def alert_on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """--sla_miss_callback: runs when a task misses its SLA deadline."""
    task_ids = [ti.task_id for ti in task_list]
    print(f"[sla_miss_callback] SLA missed for tasks: {task_ids}")
