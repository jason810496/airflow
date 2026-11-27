Raise on outlet events that Airflow used to drop silently

A task's ``outlet_events`` now raises instead of dropping events Airflow would never record:

- Looking up an asset or asset alias that is not in the task's ``outlets`` raises ``KeyError``.
  This covers ``outlet_events[...]``, ``for_asset_alias()``, ``for_asset()`` with both ``name`` and ``uri``,
  and yielded ``Metadata``. ``outlet_events.get()`` returns ``None`` and ``in`` returns ``False`` for these keys.
  An asset ref (``Asset.ref()``, or ``for_asset()`` with only ``name`` or ``uri``) is first resolved in Airflow,
  so a ref to an unknown asset raises ``AirflowRuntimeError``.
  The task can append to ``outlets`` while it runs, but an asset that Airflow does not already know
  still gets no event.
- ``add()`` only works on an asset alias accessor, so calling it on an asset accessor raises ``TypeError``.

``yield Metadata(asset, extra, alias=alias)`` writes to both the alias and the asset, so the asset must
now be in the task's ``outlets`` too. To add an asset that is not known before the task runs, call
``outlet_events[alias].add(asset, extra=...)`` instead.

* Types of change

  * [x] Dag changes
  * [ ] Config changes
  * [ ] API changes
  * [ ] CLI changes
  * [x] Behaviour changes
  * [ ] Plugin changes
  * [ ] Dependency changes
  * [ ] Code interface changes
