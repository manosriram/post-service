const app = require("express")();
const PORT = process.env.PORT || 5000;
const bp = require("body-parser");

app.use(bp.json());
app.use(bp.urlencoded({ extended: false }));

var mysql = require("mysql");
var connection = mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "password",
    database: "postv"
});
connection.connect();

const redis = require("redis");
const client = redis.createClient();
client.on("error", function(error) {
    console.error(error);
});

app.get("/user/:username", (req, res) => {
    const { username } = req.params;

    connection.query(
        "select * from post p join viewers on p.id = viewers.postid",
        username,
        (err, rows) => {
            res.send(rows);
        }
    );
});

// Create a post
app.put("/:author", (req, res) => {
    const { author } = req.params;
    connection.query(
        "insert into post(author) values(?)",
        author,
        (err, rows) => {
            client.set(rows.insertId, 0);
        }
    );
    return res.send("post added");
});

// Users who viewed the post
app.get("/viewers/:postid", (req, res) => {
    const { postid } = req.params;

    connection.query(
        "select * from viewers where postid=?",
        postid,
        (err, rows) => {
            res.send(rows);
        }
    );
});

// Increment post count in redis
app.post("/:postid", (req, res) => {
    const { postid } = req.params;
    let { username } = req.body;
    username = "?" + username + "/" + postid;

    try {
        client.exists(username, (err, exists) => {
            console.log(exists);
            if (!exists) {
                connection.query(
                    "insert into viewers(username, postid) values(?, ?)",
                    [username.split("/")[0].substr(1), postid]
                );
                client.set(username, true);
            }
        });

        client.incr(postid, (err, done) => {
            if (err) console.log(err);
        });

        return res.send("done");
    } catch (err) {
        console.log(err);
    }
});

// Flush views from redis to database;
function flushViews() {
    client.KEYS("*", (err, data) => {
        for (let t = 0; t < data.length; ++t) {
            if (data[t][0] == "?") continue;
            client.get(data[t], (err, count) => {
                connection.query(
                    "update post set views = views + ?",
                    count,
                    (err, rows) => {
                        if (err) console.log(err);
                        else {
                            console.log(`Updated Post ${data[t]}`);
                            client.set(data[t], 0);
                        }
                    }
                );
            });
        }
    });
}

// Can be a cron job.
setInterval(() => {
    flushViews();
}, 5 * 60000);

app.listen(PORT, () => console.log(`Server at ${PORT}`));
