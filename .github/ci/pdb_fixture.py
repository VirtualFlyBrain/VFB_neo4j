"""CI helper for the PDB fixture. Usage:
  pdb_fixture.py wait   ENDPOINT USER PWD        wait until the PDB answers
  pdb_fixture.py seed   ENDPOINT USER PWD        load pdb_seed.cypher
  pdb_fixture.py check  ENDPOINT USER PWD        assert what add_refs_for_anat should have written
Talks to the HTTP transactional endpoint only (no Bolt), like the pipeline does."""
import json, sys, time, base64, urllib.request, pathlib


def tx(endpoint, usr, pwd, statements):
    req = urllib.request.Request(
        endpoint.rstrip('/') + '/db/neo4j/tx/commit',
        data=json.dumps({"statements": [{"statement": s} for s in statements]}).encode(),
        headers={"Content-Type": "application/json",
                 "Authorization": "Basic " + base64.b64encode(f"{usr}:{pwd}".encode()).decode()})
    with urllib.request.urlopen(req, timeout=60) as r:
        out = json.load(r)
    if out.get("errors"):
        raise RuntimeError(out["errors"])
    return [[row["row"] for row in res["data"]] for res in out["results"]]


def main():
    cmd, endpoint, usr, pwd = sys.argv[1:5]
    if cmd == "wait":
        for i in range(60):
            try:
                tx(endpoint, usr, pwd, ["RETURN 1"])
                print(f"PDB answered after ~{i * 5}s")
                return
            except Exception as e:
                print("waiting for PDB:", str(e)[:100])
                time.sleep(5)
        sys.exit("PDB did not start within 5 minutes")
    if cmd == "seed":
        text = (pathlib.Path(__file__).parent / "pdb_seed.cypher").read_text()
        stmts = [s.strip() for s in text.split("\n;\n")]
        stmts = ["\n".join(l for l in s.splitlines() if not l.startswith("//")).strip() for s in stmts]
        stmts = [s.rstrip(";").strip() for s in stmts if s.strip().rstrip(";")]
        tx(endpoint, usr, pwd, stmts)
        print(f"seeded {len(stmts)} statements:", tx(endpoint, usr, pwd, ["MATCH (c:Class) RETURN count(c)"])[0])
        return
    if cmd == "check":
        failed = False

        def report(name, good, detail):
            nonlocal failed
            print(("PASS " if good else "FAIL ") + name, "->", detail)
            failed |= not good

        rows = tx(endpoint, usr, pwd, [
            "MATCH (c:Class)-[r:has_reference]->(p:pub) "
            "RETURN c.short_form, r.typ, r.scope, p.short_form ORDER BY c.short_form, r.typ, p.short_form"])[0]
        print("has_reference edges written:")
        for row in rows:
            print("  ", row)
        # One edge per distinct (class, pub): the neuron's definition and exact synonym cite the same pub, so they share an
        # edge. The PMID ref on the brain synonym is unsupported and the unannotated class has none.
        want = {("FBbt_00005106", "FBrf0112030"), ("FBbt_00005801", "Unattributed"), ("FBbt_00003624", "FBrf0000001")}
        got = {(r[0], r[3]) for r in rows}
        report("class -> pub has_reference edges are exactly the expected pairs", got == want,
               f"missing {sorted(want - got)} unexpected {sorted(got - want)}")
        d = tx(endpoint, usr, pwd, ["MATCH (c:Class {short_form:'FBbt_00005106'}) RETURN size(c.description)"])[0]
        report("definition copied to description", bool(d) and d[0][0] >= 1, d)
        pubs = {r[0] for r in tx(endpoint, usr, pwd, ["MATCH (p:pub) RETURN p.short_form"])[0]}
        report("expected pub nodes exist", {"FBrf0112030", "FBrf0000001", "Unattributed"} <= pubs, sorted(pubs)[:12])
        sys.exit(1 if failed else 0)
    sys.exit(f"unknown command {cmd}")


main()
