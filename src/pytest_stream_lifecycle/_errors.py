"""Attach secondary exception evidence without replacing a primary failure."""


def add_exception_notes(primary: BaseException, label: str, error: BaseException) -> None:
    seen: set[int] = set()
    pending = [error]
    while pending:
        current = pending.pop()
        if current is primary or id(current) in seen:
            continue
        seen.add(id(current))
        primary.add_note(f"{label}: {current!r}")
        for note in getattr(current, "__notes__", ()):
            primary.add_note(note)
        if isinstance(current, BaseExceptionGroup):
            pending.extend(reversed(current.exceptions))
        if current.__context__ is not None:
            pending.append(current.__context__)
        if current.__cause__ is not None:
            pending.append(current.__cause__)
