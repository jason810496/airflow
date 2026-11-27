Raise on outlet events that Airflow used to drop silently

A task's ``outlet_events`` now raises instead of dropping events Airflow would never record:

- Looking up an asset or asset alias that is not in the task's ``outlets`` raises ``KeyError``.
  This covers ``outlet_events[...]``, ``for_asset_alias()``, ``for_asset()`` with both ``name`` and ``uri``,
  and yielded ``Metadata`` without ``alias``. ``outlet_events.get()`` returns ``None`` and ``in`` returns ``False`` for these keys.
  Airflow looks up an asset ref (``Asset.ref()``, or ``for_asset()`` with only ``name`` or ``uri``) by name or URI first,
  so a ref to an asset it does not know raises ``AirflowRuntimeError``.
  An asset ref in ``outlets``, such as ``Asset.ref(name="x")``, only matches the asset Airflow finds for it,
  so an ``Asset`` with the same name but another URI raises ``KeyError``.
  The task can append to ``outlets`` while it runs, but an asset that Airflow does not already know
  still gets no event.
- ``add()`` only works on an asset alias accessor, so calling it on an asset accessor raises ``TypeError``.

``yield Metadata(asset, extra, alias=alias)`` still works for an asset that is not in ``outlets``, as long as
the alias is.

* Types of change

  * [x] Dag changes
  * [ ] Config changes
  * [ ] API changes
  * [ ] CLI changes
  * [x] Behaviour changes
  * [ ] Plugin changes
  * [ ] Dependency changes
  * [ ] Code interface changes
