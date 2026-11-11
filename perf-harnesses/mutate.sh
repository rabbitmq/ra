#!/bin/sh
# Mutation-tests the ra_seq property suite: reintroduce each known
# regression and confirm the properties reject it.
set -e
cd "$(dirname "$0")/mut"

RA_EBIN=/Users/kn023463/code/rabbitmq/ra/_build/test/lib/ra/ebin
DEPS=/Users/kn023463/code/rabbitmq/ra/_build/test/lib
# NB: mutant ebin must come LAST so that it shadows ra's own ra_seq
PA="-pa $RA_EBIN -pa $DEPS/proper/ebin -pa ebin"

build() {
    erlc -I . -o ebin ra_seq.erl 2>/dev/null
    erlc -I . -o ebin -pa "$DEPS/proper/ebin" ra_seq_SUITE.erl 2>/dev/null
}

# run one property; print PASS if it holds, FAIL if it is rejected
check() {
    if erl -noshell $PA -eval \
        "try ra_seq_SUITE:$1([]) of _ -> io:format(\"PASS~n\") catch _:_ -> io:format(\"FAIL~n\") end, halt()." \
        2>/dev/null | grep -q PASS; then
        echo "    $1: PASS (property held)"
    else
        echo "    $1: FAIL (property rejected the mutant)"
    fi
}

echo "=== baseline (correct implementation) ==="
cp ra_seq.erl ra_seq.erl.orig
build
for p in prop_add_model prop_add_canonical prop_add_appendable \
         prop_remove_prefix_model prop_remove_prefix_canonical \
         prop_remove_prefix_roundtrip prop_remove_prefix_detects_gaps; do
    check $p
done

echo
echo "=== mutant 1: add/2 builds non-canonical 2-element ranges ==="
cp ra_seq.erl.orig ra_seq.erl
# make push_range always form a range on adjacency, even for 2 indexes
perl -0pi -e 's/push_range\(S, E, \[A \| Rem\]\) when is_integer\(A\) andalso S == A \+ 1 ->\n    case E > S of\n        true ->\n            %% A, S, S\+1\.\.\. is at least three consecutive indexes\n            \[\{A, E\} \| Rem\];\n        false ->\n            \[S, A \| Rem\]\n    end;/push_range(S, E, [A | Rem]) when is_integer(A) andalso S == A + 1 ->\n    [{A, E} | Rem];/' ra_seq.erl
grep -q 'push_range(S, E, \[A | Rem\]) when is_integer(A) andalso S == A + 1 ->\n' ra_seq.erl || true
build
for p in prop_add_model prop_add_canonical prop_add_appendable; do
    check $p
done

echo
echo "=== mutant 2: remove_prefix/2 skips the coverage check ==="
cp ra_seq.erl.orig ra_seq.erl
perl -0pi -e 's/    case covers\(lists:reverse\(Prefix\),\n                lists:reverse\(limit\(PrefLast, Seq\)\)\) of\n        true ->\n            \{ok, floor\(PrefLast \+ 1, Seq\)\};\n        false ->\n            \{error, not_prefix\}\n    end\./    \{ok, floor(PrefLast + 1, Seq)\}./' ra_seq.erl
build
for p in prop_remove_prefix_model prop_remove_prefix_roundtrip prop_remove_prefix_detects_gaps; do
    check $p
done

echo
echo "=== mutant 3: remove_prefix/2 is off by one (drops one index too many) ==="
cp ra_seq.erl.orig ra_seq.erl
perl -0pi -e 's/            \{ok, floor\(PrefLast \+ 1, Seq\)\};/            \{ok, floor(PrefLast + 2, Seq)\};/' ra_seq.erl
build
for p in prop_remove_prefix_model prop_remove_prefix_roundtrip; do
    check $p
done

echo
echo "=== mutant 4: add/2 forgets to limit To below first(Add) ==="
cp ra_seq.erl.orig ra_seq.erl
perl -0pi -e 's/    merge0\(lists:reverse\(Add\), limit\(Fst - 1, To\)\)\./    _ = Fst,\n    merge0(lists:reverse(Add), To)./' ra_seq.erl
build
for p in prop_add_model prop_add_canonical; do
    check $p
done

cp ra_seq.erl.orig ra_seq.erl
echo
echo "done"
