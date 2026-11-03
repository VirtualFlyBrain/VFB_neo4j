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
        checks = {
            "definition copied to description":
                ("MATCH (c:Class {short_form:'FBbt_00005106'}) RETURN size(c.description)", lambda v: v >= 1),
            "pub nodes written":
                ("MATCH (p:pub) RETURN count(p)", lambda v: v >= 3),
            "class -> pub has_reference edges written":
                ("MATCH (:Class)-[r:has_reference]->(:pub) RETURN count(r)", lambda v: v >= 4),
            "unannotated class left alone":
                ("MATCH (c:Class {short_form:'FBbt_00000001'})-[r:has_reference]->(:pub) RETURN count(r)", lambda v: v == 0),
        }
        print("has_reference edges written:")
        for row in tx(endpoint, usr, pwd, [
                "MATCH (c:Class)-[r:has_reference]->(p:pub) "
                "RETURN c.short_form, r.typ, r.scope, p.short_form ORDER BY c.short_form, r.typ, p.short_form"])[0]:
            print("  ", row)
        failed = False
        for name, (q, ok) in checks.items():
            rows = tx(endpoint, usr, pwd, [q])[0]
            v = rows[0][0] if rows else None
            good = v is not None and ok(v)
            print(("PASS " if good else "FAIL ") + name, "->", v)
            failed |= not good
        sys.exit(1 if failed else 0)
    sys.exit(f"unknown command {cmd}")


main()
