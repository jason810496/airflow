Raise on outlet events that Airflow used to drop silently

A task's ``outlet_events`` now raises instead of dropping events Airflow would never record:

- Looking up an asset or asset alias that is not in the task's ``outlets`` raises ``KeyError``.
  This covers ``outlet_events[...]``, ``for_asset()``, ``for_asset_alias()`` and yielded ``Metadata``.
  An asset added to ``outlets`` while the task runs is accepted.
- Calling ``add()`` on an asset accessor, not an asset alias accessor, raises ``TypeError``.

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
