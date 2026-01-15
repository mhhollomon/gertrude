from gertrude.lib.types.colref import ColRef

def test_colref_fullname() :
    assert ColRef("foo").full_name() == "foo"
    assert ColRef("foo", "bar").full_name() == "bar.foo"

def test_colref_matchedby() :
    assert not ColRef("foo").matchedBy(ColRef("foo", "bar"))
    assert ColRef("foo", "bar").matchedBy(ColRef("foo"))
    assert not ColRef("foo", "bar").matchedBy(ColRef("foo", "mad"))
    assert ColRef("foo").matchedBy(ColRef("foo"))
    assert not ColRef("foo").matchedBy(ColRef("bar"))
    assert not ColRef("foo").matchedBy(ColRef("bar", "baz"))

def test_colref_hashable() :
    assert hash(ColRef("foo")) == hash(ColRef("foo"))
    assert hash(ColRef("foo")) != hash(ColRef("bar"))

    assert hash(ColRef("foo", "bar")) == hash(ColRef("foo", "bar"))
    assert hash(ColRef("foo", "bar")) != hash(ColRef("foo", "baz"))

    # If hashable, then it is usable as a key in a dictionary.
    x = { ColRef("foo") : 1 }